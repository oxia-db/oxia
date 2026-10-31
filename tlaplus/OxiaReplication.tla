
-------------------------- MODULE OxiaReplication --------------------------
EXTENDS MessagePassing, Integers, Sequences, FiniteSets, TLC

(*

Each action has ample comments that describe it. Also each action is
clearly commented to show what the enabling conditions are and what
the state changes are. Enabling conditions are the conditions required
for the action to take place, such as an term being of the right value.
An action wil only occur if all the enabling conditions are true.

This spec includes:
- One or more coordinators
- Multiple nodes
- Leader election
- Data replication
- Changes of the ensemble: a one-for-one swap of a member, from -> to, with
  a joint fence of the ensembles before and after the change, as Raft's joint
  consensus

Not modelling:
- Failure detection. There is no need to as:
    - the coordinator can start an election at any time.
    - for each election, the coordinator has visibility of any arbitrary number of nodes which
      simulates an arbitray number of node failures.

The state space is large as expected. Use of simulation for non-trivial numbers of nodes, values
and terms is required.
*)

CONSTANTS Coordinators,         \* The set of all coordinators (should only be one but the protocol should handle multiple)
          Nodes,             \* The set of all nodes
          Values,            \* The set of all values that can be sent by clients
          RepFactor          \* The desired initial replication factor

\* State space
CONSTANTS MaxTerms,         \* The max number of terms for the model checker to explore
          MaxCoordinatorStops   \* The max number of coordinator crashes that the model checker will explore

\* Shard statuses
CONSTANTS STEADY_STATE,      \* The shard is in data replication mode
          ELECTION           \* An election is in progress

\* Coordinator statuses
CONSTANTS RUNNING,           \* an coordinator is running
          NOT_RUNNING        \* an coordinator is not running

\* Node statuses
CONSTANTS FENCED,            \* denotes a node that has been fenced during an election
          LEADER,            \* denotes a chosen leader
          FOLLOWER,          \* denotes a follower ready to receive entries from the leader
          NOT_MEMBER         \* not a member of the shard ensemble

\* Election phases
CONSTANTS FENCING,           \* prevent nodes from making progress
          NOTIFY_LEADER,     \* a leader has been selected and is being notified by the coordinator
          LEADER_ELECTED     \* a leader has confirmed its leadership

\* return codes
CONSTANTS OK,                \* the request was serviced correctly
          INVALID_TERM      \* node has rejected a request because it has a higher term than the request

\* follow cursor states
CONSTANTS ATTACHED,          \* a follow cursor is attached and operational
          PENDING_TRUNCATE   \* a follow cursor is inactive but will become active upon follower truncation

\* other model values
CONSTANTS NIL                \* used in various places to signify something does not exist

VARIABLES
          \* state of the system
          metadata_version,  \* monotonic counter incremented on each metadata change
          metadata,          \* the metadata shard state
          node_state,        \* the nodes and their state
          coordinator_state,    \* the coordinators and their state

          \* auxilliary state required by the spec
          confirmed,         \* the confirmation status of each written value
          coordinator_stop_ctr  \* a counter that incremented each time an coordinator crashes/stops

vars == << metadata_version, metadata, node_state, coordinator_state, confirmed, messages, coordinator_stop_ctr >>
view == << metadata_version, metadata, node_state, coordinator_state, confirmed, messages >>

\* The values and the coordinators are interchangeable: a state checked once
\* covers its permutations. Only sound for the safety properties
symmetry == Permutations(Values) \union Permutations(Coordinators)

(* -----------------------------------------
   TYPE DEFINITIONS

  The type definitions for the fundamental structures, messages
  and state of the various actors. Required for the optional type invariant
  TypeOK and also useful to understand the structure of the protocol.
  ----------------------------------------- *)

\* Basic types
Term == 0..MaxTerms
NodesOrNil == Nodes \union {NIL}
EntryId == [offset: Nat, term: Term]
LogEntry == [entry_id: EntryId, value: Values]
CursorStatus == { NIL, ATTACHED, PENDING_TRUNCATE }
Cursor == [status: CursorStatus, last_pushed: EntryId, last_confirmed: EntryId]
NodeStatus == { LEADER, FOLLOWER, FENCED, NOT_MEMBER }

\* equivalent of null for various record types
NoEntryId == [offset |-> 0, term |-> 0]
NoLogEntry == [entry_id |-> NoEntryId, value |-> NIL]
NoCursor == [status |-> NIL, last_pushed |-> NoEntryId, last_confirmed |-> NoEntryId]

\* ------------------------------
\* Messages
\* ------------------------------

\* Coordinator -> Nodes
NewTermRequest ==
    [type:     {NEW_TERM_REQUEST},
     node:     Nodes,
     coordinator: Coordinators,
     term:    Term]

\* Node -> Coordinator
NewTermResponse ==
    [type:       {NEW_TERM_RESPONSE},
     node:       Nodes,
     coordinator:   Coordinators,
     head_entry_id: EntryId,
     term:      Term]

\* Coordinator -> Node
BecomeLeaderRequest ==
    [type:         {BECOME_LEADER_REQUEST},
     node:         Nodes,
     coordinator:     Coordinators,
     term:        Term,
     rep_factor:   Nat,
     follower_map: [Nodes -> EntryId \union {NIL}]]

\* Node -> Coordinator
BecomeLeaderResponse ==
    [type:     {BECOME_LEADER_RESPONSE},
     node:     Nodes,
     coordinator: Coordinators,
     term:    Term]

\* Coordinator -> Node (Leader)
AddFollowerRequest ==
    [type:       {ADD_FOLLOWER_REQUEST},
     node:       Nodes,
     follower:   Nodes,
     head_entry_id: EntryId,
     term:      Term]

\* Node (Leader) -> Node (Follower)
TruncateRequest ==
    [type:        {TRUNCATE_REQUEST},
     dest_node:   Nodes,
     source_node: Nodes,
     term:       Term,
     head_entry_id:  EntryId]

\* Node (Follower) -> Node (Leader)
TruncateResponse ==
    [type:        {TRUNCATE_RESPONSE},
     dest_node:   Nodes,
     source_node: Nodes,
     term:       Term,
     head_entry_id:  EntryId]

\* Node (Leader) -> Node (Follower)
AppendEntry ==
    [type:         {APPEND},
     dest_node:    Nodes,
     source_node:  Nodes,
     entry:        LogEntry,
     commit_entry_id: EntryId,
     term:        Term]

\* Node (Follower) -> Node (Leader)
Ack ==
    [type:        {ACK},
     dest_node:   Nodes,
     source_node: Nodes,
     code:        {OK, INVALID_TERM},
     entry_id:    EntryId,
     term:       Term]

Message ==
    NewTermRequest \union NewTermResponse \union
    BecomeLeaderRequest \union BecomeLeaderResponse \union
    AddFollowerRequest \union
    TruncateRequest \union TruncateResponse \union
    AppendEntry \union Ack

\* Metadata store state
ShardStatus == { NIL, STEADY_STATE, ELECTION }

\* A pending change of the ensemble: to replaced from in the ensemble
EnsembleChange == [from: Nodes, to: Nodes]

Metadata == [shard_status: ShardStatus,
             term:        Term,
             ensemble:     SUBSET Nodes,
             rep_factor:   Nat,
             leader:       NodesOrNil,
             change:       EnsembleChange \union {NIL}]

\* Node and Coordinator state
NodeState == [id:              Nodes,
              status:          NodeStatus,
              term:           Term,
              leader:          Nodes \union {NIL},
              rep_factor:      Nat,
              log:             SUBSET LogEntry,
              commit_entry_id:    EntryId,
              head_entry_id:      EntryId,
              follow_cursor:   [Nodes -> Cursor]]

CoordinatorStatuses == {NOT_RUNNING, RUNNING}
ElectionPhases == { NIL, FENCING, NOTIFY_LEADER, LEADER_ELECTED }
CoordinatorState == [id:                        Coordinators,
                  status:                    CoordinatorStatuses,
                  md_version:                Nat,
                  md:                        Metadata \union {NIL},
                  election_phase:            ElectionPhases,
                  election_leader:           Nodes \union {NIL},
                  election_leader_head:      EntryId,
                  election_fence_responses:  SUBSET NewTermResponse]

\* Type invariant
TypeOK ==
    /\ metadata \in Metadata
    /\ node_state \in [Nodes -> NodeState]
    /\ coordinator_state \in [Coordinators -> CoordinatorState]
    /\ \A v \in DOMAIN confirmed :
        /\ v \in Values
        /\ confirmed[v] \in BOOLEAN
    /\ \A msg \in DOMAIN messages : msg \in Message


(* -----------------------------------------
   INITIAL STATE

   The only initial state is the shard ensemble
   and rep factor. There is no leader, followers
   or even a running coordinator.
   -----------------------------------------
*)

Init ==
    LET ensemble == CHOOSE e \in SUBSET Nodes : Cardinality(e) = RepFactor
    IN
        /\ metadata_version = 0
        /\ metadata = [shard_status |-> NIL,
                       term        |-> 0,
                       ensemble     |-> ensemble,
                       rep_factor   |-> RepFactor,
                       leader       |-> NIL,
                       change       |-> NIL]
        /\ node_state = [n \in Nodes |->
                            [id              |-> n,
                             status          |-> NOT_MEMBER,
                             term           |-> 0,
                             leader          |-> NIL,
                             rep_factor      |-> 0,
                             log             |-> {},
                             commit_entry_id    |-> NoEntryId,
                             head_entry_id      |-> NoEntryId,
                             follow_cursor   |-> [n1 \in Nodes |-> NoCursor]]]
        /\ coordinator_state = [o \in Coordinators |->
                            [id                        |-> o,
                             status                    |-> NOT_RUNNING,
                             md_version                |-> 0,
                             md                        |-> NIL,
                             election_phase            |-> NIL,
                             election_leader           |-> NIL,
                             election_leader_head      |-> NoEntryId,
                             election_fence_responses  |-> {}]]
        /\ confirmed = <<>>
        /\ messages = <<>>
        /\ coordinator_stop_ctr = 0

(* -----------------------------------------
   HELPER OPERATORS
   -----------------------------------------*)

Majority(n) == (n \div 2) + 1

\* The smallest number of the n members that overlaps every majority of them:
\* a majority leaves out n - Majority(n) members at most.
MajorityOverlap(n) == n - Majority(n) + 1

\* The ensemble before the pending change: from instead of to
OldEnsemble(md) ==
    IF md.change = NIL
    THEN md.ensemble
    ELSE (md.ensemble \ {md.change.to}) \union {md.change.from}

\* Does this set of fenced nodes reach the quorum of the fence? As Raft's joint
\* consensus, it overlaps every majority of the ensemble before the change,
\* which holds the entries committed before it, and every majority of the
\* ensemble, which holds the ones committed since.
IsFenceQuorum(nodes, md) ==
    LET n == Cardinality(md.ensemble)
    IN /\ Cardinality(nodes \intersect OldEnsemble(md)) >= MajorityOverlap(n)
       /\ Cardinality(nodes \intersect md.ensemble) >= Majority(n)

\* Compare two entry ids:
\*  entry_id1 > entry_id2 = 1
\*  entry_id1 = entry_id2 = 0
\*  entry_id1 < entry_id2 = -1
CompareLogEntries(entry_id1, entry_id2) ==
    IF entry_id1.term > entry_id2.term THEN 1
    ELSE
        IF entry_id1.term = entry_id2.term /\ entry_id1.offset > entry_id2.offset THEN 1
        ELSE
            IF entry_id1.term = entry_id2.term /\ entry_id1.offset = entry_id2.offset THEN 0
            ELSE -1

IsLowerId(entry_id1, entry_id2) ==
    CompareLogEntries(entry_id1, entry_id2) = -1

IsLowerOrEqualId(entry_id1, entry_id2) ==
    CompareLogEntries(entry_id1, entry_id2) \in {-1, 0}

IsHigherId(entry_id1, entry_id2) ==
    CompareLogEntries(entry_id1, entry_id2) = 1

IsHigherOrEqualId(entry_id1, entry_id2) ==
    CompareLogEntries(entry_id1, entry_id2) \in {0, 1}

IsSameId(entry_id1, entry_id2) ==
    CompareLogEntries(entry_id1, entry_id2) = 0

HeadEntry(log) ==
    IF log = {}
    THEN NoLogEntry
    ELSE CHOOSE entry \in log :
        \A e \in log : IsHigherOrEqualId(entry.entry_id, e.entry_id)

(*---------------------------------------------------------
  Coordinator starts

  The coordinator loads the metadata from the metadata store.
  The metadata will decide what the coordinator might do
  next.
----------------------------------------------------------*)

CoordinatorStarts ==
    \* enabling conditions
    /\ \E o \in Coordinators :
        /\ coordinator_state[o].status = NOT_RUNNING
        \* state changes
        /\ coordinator_state' = [coordinator_state EXCEPT ![o] =
            [id                        |-> o,
             status                    |-> RUNNING,
             md_version                |-> metadata_version,
             md                        |-> metadata,
             election_phase            |-> NIL,
             election_leader           |-> NIL,
             election_leader_head      |-> NoEntryId,
             election_fence_responses  |-> {}]]
        /\ UNCHANGED << confirmed, metadata, metadata_version, node_state, messages, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Coordinator stops

  The coordinator crashes/stops and loses all local state.
----------------------------------------------------------*)

CoordinatorStops ==
    \* enabling conditions
    /\ coordinator_stop_ctr < MaxCoordinatorStops
    /\ \E o \in Coordinators :
        /\ coordinator_state[o].status = RUNNING
        \* state changes
        /\ coordinator_state' = [coordinator_state EXCEPT ![o].status                    = NOT_RUNNING,
                                                    ![o].md_version                = 0,
                                                    ![o].md                        = NIL,
                                                    ![o].election_phase            = NIL,
                                                    ![o].election_leader           = NIL,
                                                    ![o].election_leader_head      = NoEntryId,
                                                    ![o].election_fence_responses  = {}]
        /\ coordinator_stop_ctr' = coordinator_stop_ctr + 1
        /\ CoordinatorMessagesLost(o)
        /\ UNCHANGED << confirmed, metadata, metadata_version, node_state >>


(*---------------------------------------------------------
  Coordinator starts an election

  In reality this will be triggered by some kind of failure
  detection outside the scope of this spec. This
  spec simply allows the coordinator to decide to start an
  election at anytime.

  The first phase of an election is for the coordinator to
  ensure that the shard cannot make progress by fencing
  a majority of nodes and getting their head entry_id.

  Key points:
  - An election increments the term of the shard.

  - The message passing module assumes all sent messages are
    retried until abandoned by the sender. The coordinator elects a leader
    once any majority responded, which simulates message loss and dead
    nodes of a minority.

  - In this spec, updating the metadata in the metadata store and sending
    fencing requests is one atomic action. In an implementation, the
    metadata would need to be updated first, then the fencing requests sent.

  - The election phases are local state for the coordinator only. The only
    required state in the metadata is the term and that an election is
    in-progress. If the coordinator were to fail at any time during an
    election then the next coordinator that starts would see the ELECTION
    status in the metadata and trigger a new election.

  - The metadata update will fail if the version does not match the version
    held by the coordinator.

  - The election may change the ensemble, when no change is pending: it
    replaces a member, from, with a node outside of the ensemble, to. As the
    implementation, the election persists only the new term at first, and
    the change with the new leader: until then, the change only lives in the
    election. Once persisted, the change stays pending until it is finished,
    and each election fences from too, until then: as Raft's joint consensus,
    the fence reaches a quorum of both the ensemble before the change and the
    ensemble after it.
----------------------------------------------------------*)

GetNewTermRequests(o, new_term, ensemble) ==
    { [type     |-> NEW_TERM_REQUEST,
       node     |-> n,
       coordinator |-> o,
       term    |-> new_term] : n \in ensemble }

\* No change, or a change of the ensemble when none is pending
ElectionChanges(md) ==
    {NIL} \union
    IF md.change = NIL
    THEN { [from |-> from, to |-> to] : from \in md.ensemble, to \in Nodes \ md.ensemble }
    ELSE {}

ApplyChange(md, change) ==
    IF change = NIL
    THEN md
    ELSE [md EXCEPT !.ensemble = (@ \ {change.from}) \union {change.to},
                    !.change   = change]

CoordinatorStartsElection ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate           == coordinator_state[o]
        IN
            /\ metadata.term < MaxTerms \* state space reduction
            /\ ostate.status = RUNNING
            /\ ostate.md_version = metadata_version
            /\ \E change \in ElectionChanges(metadata) :
                \* state changes
                /\ LET new_term      == metadata.term + 1
                       new_metadata   == [metadata EXCEPT !.term        = new_term,
                                                          !.shard_status = ELECTION,
                                                          !.leader       = NIL]
                       election_md    == ApplyChange(new_metadata, change)
                       new_md_version == metadata_version + 1
                   IN
                       /\ metadata_version' = new_md_version
                       /\ metadata' = new_metadata
                       /\ coordinator_state' = [coordinator_state EXCEPT ![o].id                        = o,
                                                                   ![o].md_version                = new_md_version,
                                                                   ![o].md                        = election_md,
                                                                   ![o].election_phase            = FENCING,
                                                                   ![o].election_leader           = NIL,
                                                                   ![o].election_leader_head      = NoEntryId,
                                                                   ![o].election_fence_responses  = {}]
                       /\ SendMessages(GetNewTermRequests(o, new_term,
                                                          election_md.ensemble \union OldEnsemble(election_md)))
    /\ UNCHANGED << node_state, confirmed, coordinator_stop_ctr >>


(*---------------------------------------------------------
  Node handles a fence request

  A node receives a fencing request, fences itself and responds
  with its head entry_id.

  When a node is fenced it cannot:
  - accept any writes from a client.
  - accept add entry requests from a leader.
  - send any entries to followers if it was a leader.

  Any existing follow cursors are destroyed.
----------------------------------------------------------*)
GetNewTermResponse(nstate, new_term, o) ==
    [type           |-> NEW_TERM_RESPONSE,
     node           |-> nstate.id,
     coordinator       |-> o,
     head_entry_id     |-> nstate.head_entry_id,
     term          |-> new_term]

NodeHandlesFencingRequest ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, NEW_TERM_REQUEST)
        /\ LET nstate == node_state[msg.node]
           IN /\ nstate.term < msg.term
              \* state changes
              /\ node_state' = [node_state EXCEPT ![msg.node] =
                                    [@ EXCEPT !.term           = msg.term,
                                              !.status          = FENCED,
                                              !.rep_factor      = 0,
                                              !.follow_cursor   = [n \in Nodes |-> NoCursor]]]
              /\ ProcessedOneAndSendAnother(msg, GetNewTermResponse(nstate, msg.term, msg.coordinator))
              /\ UNCHANGED << metadata, metadata_version, coordinator_state, confirmed, coordinator_stop_ctr >>


(*---------------------------------------------------------
  Coordinator handles a fence response during the fencing

  The coordinator stores the fence response. It may keep storing
  responses after reaching the quorum, as the implementation waits
  a grace period for the remaining nodes.
----------------------------------------------------------*)

CoordinatorHandlesFencingResponse ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate == coordinator_state[o]
        IN
            /\ ostate.status = RUNNING
            /\ ostate.md.shard_status = ELECTION
            /\ ostate.election_phase = FENCING
            /\ \E msg \in DOMAIN messages :
                /\ ReceivableMessageOfType(messages, msg, NEW_TERM_RESPONSE)
                /\ msg.coordinator = o
                /\ ostate.md.term = msg.term
                \* state changes
                /\ coordinator_state' = [coordinator_state EXCEPT ![o].election_fence_responses = @ \union { msg }]
                /\ MessageProcessed(msg)
                /\ UNCHANGED << metadata, metadata_version, node_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Coordinator elects a leader once the fence reaches quorum

  The coordinator chooses the leader among the fenced members of
  the ensemble with the highest head entry_id, which has every
  committed entry. With a pending change of the ensemble:
  - from strictly ahead of the fenced members: the change is
    undone, from swaps back with to, and is the leader.
  - to tied at the highest: the change is finished.

  As the implementation, the coordinator persists the new leader,
  the ensemble and the change before it notifies the leader: once
  the leader commits entries on a majority of the new ensemble, the
  next elections must fence it.
----------------------------------------------------------*)

FollowerResponse(responses, f) ==
    CHOOSE r \in responses : r.node = f

GetFollowerMap(leader, responses, final_ensemble) ==
    LET followers == { f \in final_ensemble :
                        /\ f # leader
                        /\ \E r \in responses : r.node = f }
    IN  [f \in Nodes |->
            IF f \in followers
            THEN FollowerResponse(responses, f).head_entry_id
            ELSE NIL]

GetBecomeLeaderRequest(n, term, o, follower_map, rep_factor) ==
    [type         |-> BECOME_LEADER_REQUEST,
     node         |-> n,
     coordinator     |-> o,
     follower_map |-> follower_map,
     rep_factor   |-> rep_factor,
     term        |-> term]

\* The responses with the highest head entry_id
HighestResponses(responses) ==
    { r1 \in responses :
        ~\E r2 \in responses : IsHigherId(r2.head_entry_id, r1.head_entry_id) }

\* The ensemble and the change once the leader is chosen
UndoChange(md) ==
    [md EXCEPT !.ensemble = (@ \ {md.change.to}) \union {md.change.from},
               !.change   = NIL]

FinishChange(md) ==
    [md EXCEPT !.change = NIL]

CoordinatorElectsLeader ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate             == coordinator_state[o]
            md                 == ostate.md
            responses          == ostate.election_fence_responses
            ensemble_responses == { r \in responses : r.node \in md.ensemble }
            from_responses     == { r \in responses : md.change # NIL /\ r.node = md.change.from }
            \* the ensemble wins a tie
            undo               == /\ from_responses # {}
                                  /\ HighestResponses(ensemble_responses \union from_responses) = from_responses
            candidates         == IF undo THEN from_responses ELSE HighestResponses(ensemble_responses)
            finish             == /\ ~undo
                                  /\ md.change # NIL
                                  /\ \E r \in candidates : r.node = md.change.to
            final_md           == IF undo THEN UndoChange(md)
                                  ELSE IF finish THEN FinishChange(md)
                                  ELSE md
        IN
            /\ ostate.status = RUNNING
            /\ md.shard_status = ELECTION
            /\ ostate.election_phase = FENCING
            /\ metadata_version = ostate.md_version
            /\ IsFenceQuorum({ r.node : r \in responses }, md)
            \* the implementation chooses any of them
            /\ \E leader_res \in candidates :
                LET new_leader   == leader_res.node
                    new_md       == [final_md EXCEPT !.shard_status = STEADY_STATE,
                                                     !.leader       = new_leader,
                                                     !.rep_factor   = Cardinality(final_md.ensemble)]
                    follower_map == GetFollowerMap(new_leader, responses, new_md.ensemble)
                IN
                    \* state changes
                    /\ metadata' = new_md
                    /\ metadata_version' = metadata_version + 1
                    /\ coordinator_state' = [coordinator_state EXCEPT ![o] =
                                                [@ EXCEPT !.election_phase       = NOTIFY_LEADER,
                                                          !.election_leader      = new_leader,
                                                          !.election_leader_head = leader_res.head_entry_id,
                                                          !.md_version           = metadata_version + 1,
                                                          !.md                   = new_md]]
                    /\ SendMessage(GetBecomeLeaderRequest(new_leader,
                                                          md.term,
                                                          o,
                                                          follower_map,
                                                          new_md.rep_factor))
                    /\ UNCHANGED << node_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Node handles a Become Leader request

  The node inspects the head entry_id of each follower and
  compares it to its own head entry_id, and then either:
  - Attaches a follow cursor for the follower the head entry_ids
    have the same term, but the follower offset is lower or equal.
  - Sends a truncate request to the follower if its head
    entry_id term does not match the leader's head entry_id term or has
    a higher offset.
    The leader finds the highest entry id in its log prefix (of the
    follower head entry_id) and tells the follower to truncate it's log
    to that entry.

  Key points:
  - The election only requires a majority to complete and so the
    Become Leader request will likely only contain a majority,
    not all the nodes.
  - No followers in the Become Leader message "follower map" will
    have a higher head entry_id than the leader (as the leader was
    chosen because it had the highest head entry_id of the majority
    that responded to the fencing requests first). But as the leader
    receives more fencing responses from the remaining minority,
    the new leader will be informed of these followers and it is
    possible that their head entry_id is higher than the leader and
    therefore need truncating.
  - As Raft's figure 8, the new leader can't commit the entries of
    prior terms by counting the followers that have them: another
    leader may still overwrite them. They are only committed with an
    entry of its term, see EntryIsCommitted. The implementation
    appends a no-op entry of its term on becoming leader, to commit
    them without waiting for a write; the spec leaves it to the next
    write.
----------------------------------------------------------*)

GetHighestEntryOfTerm(nstate, target_term) ==
    IF \E entry \in nstate.log : entry.entry_id.term <= target_term
    THEN CHOOSE entry \in nstate.log :
        /\ entry.entry_id.term <= target_term
        /\ \A e \in nstate.log :
            \/ e.entry_id.term > target_term
            \/ /\ e.entry_id.term <= target_term
               /\ IsHigherOrEqualId(entry.entry_id, e.entry_id)
    ELSE NoLogEntry

NeedsTruncation(nstate, head_entry_id) ==
    \/ /\ head_entry_id.term = nstate.head_entry_id.term
       /\ head_entry_id.offset > nstate.head_entry_id.offset
    \/ head_entry_id.term # nstate.head_entry_id.term

GetCursor(nstate, head_entry_id) ==
    IF NeedsTruncation(nstate, head_entry_id)
    THEN [status         |-> PENDING_TRUNCATE,
          last_pushed    |-> NoEntryId,
          last_confirmed |-> NoEntryId]
    ELSE [status         |-> ATTACHED,
          last_pushed    |-> head_entry_id,
          last_confirmed |-> head_entry_id]

GetCursors(nstate, follower_map) ==
    [n \in Nodes |->
         IF follower_map[n] # NIL
         THEN GetCursor(nstate, follower_map[n])
         ELSE NoCursor]


GetTruncateRequest(nstate, follower, target_term) ==
    [type        |-> TRUNCATE_REQUEST,
     dest_node   |-> follower,
     source_node |-> nstate.id,
     head_entry_id  |-> GetHighestEntryOfTerm(nstate, target_term).entry_id,
     term       |-> nstate.term]

FollowersToTruncate(nstate, follower_map) ==
    { f \in DOMAIN follower_map :
            /\ follower_map[f] # NIL
            /\ NeedsTruncation(nstate, follower_map[f]) }

GetTruncateRequests(nstate, follower_map) ==
    {
        GetTruncateRequest(nstate, f, follower_map[f].term)
            : f \in FollowersToTruncate(nstate, follower_map)
    }

GetBecomeLeaderResponse(n, term, o) ==
    [type     |-> BECOME_LEADER_RESPONSE,
     term    |-> term,
     node     |-> n,
     coordinator |-> o]

NodeHandlesBecomeLeaderRequest ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, BECOME_LEADER_REQUEST)
        /\ node_state[msg.node].term = msg.term
        /\ LET truncate_requests == GetTruncateRequests(node_state[msg.node], msg.follower_map)
               leader_response   == GetBecomeLeaderResponse(msg.node, msg.term, msg.coordinator)
               updated_cursors   == GetCursors(node_state[msg.node], msg.follower_map)
           IN
              \* state changes
              /\ node_state' = [node_state EXCEPT ![msg.node] =
                                          [@ EXCEPT !.status        = LEADER,
                                                    !.leader        = msg.node,
                                                    !.rep_factor    = msg.rep_factor,
                                                    !.follow_cursor = updated_cursors]]
              /\ ProcessedOneAndSendMore(msg, truncate_requests \union {leader_response})
              /\ UNCHANGED << metadata, metadata_version, coordinator_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Coordinator handles Become Leader response

  The election is completed on receiving the Become Leader
  response. The metadata already holds the new leader.

  Key points:
  - A minority of followers may still not have been fenced yet.
    The coordinator will continue to try to fence these followers
    forever until either they respond with a fencing request,
    or a new election is performed.
----------------------------------------------------------*)

CoordinatorHandlesBecomeLeaderResponse ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate == coordinator_state[o]
        IN
            /\ ostate.status = RUNNING
            /\ \E msg \in DOMAIN messages :
                /\ ReceivableMessageOfType(messages, msg, BECOME_LEADER_RESPONSE)
                /\ msg.coordinator = o
                /\ ostate.election_phase = NOTIFY_LEADER
                /\ ostate.md.term = msg.term
                \* state changes
                /\ coordinator_state' = [coordinator_state EXCEPT ![o].election_phase = LEADER_ELECTED]
                /\ MessageProcessed(msg)
                /\ UNCHANGED << metadata, metadata_version, node_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Coordinator handles post quorum fence response

  The coordinator handles a fencing response at a time after
  it has selected a new leader. The leader must have confirmed
  its leadership before the opertator acts on any late
  follower fence responses.

  Each fence response will translate to an Add Follower
  request being sent to the leader, including the follower's
  head entry_id.
----------------------------------------------------------*)

GetAddFollowerRequest(n, term, follower, head_entry_id) ==
    [type       |-> ADD_FOLLOWER_REQUEST,
     node       |-> n,
     follower   |-> follower,
     head_entry_id |-> head_entry_id,
     term      |-> term]

CoordinatorHandlesPostQuorumFencingResponse ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate == coordinator_state[o]
        IN
            /\ ostate.status = RUNNING
            /\ ostate.election_phase = LEADER_ELECTED
            /\ \E msg \in DOMAIN messages :
                /\ ReceivableMessageOfType(messages, msg, NEW_TERM_RESPONSE)
                /\ msg.coordinator = o
                /\ msg.node \in ostate.md.ensemble \* might be the old_node
                /\ ostate.md.term = msg.term
                \* state changes
                /\ coordinator_state' = [coordinator_state EXCEPT ![o].election_fence_responses = @ \union {msg}]
                /\ ProcessedOneAndSendAnother(msg,
                                              GetAddFollowerRequest(ostate.election_leader,
                                                                    ostate.md.term,
                                                                    msg.node,
                                                                    msg.head_entry_id))
            /\ UNCHANGED << metadata, metadata_version, node_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Coordinator finishes the change of the ensemble

  Once a majority of the ensemble, the leader included, has
  committed up to the head of the leader at the election, the
  ensemble holds every committed entry on a majority without
  from: the coordinator finishes the pending change, and the next
  elections no longer fence from.

  Key points:
  - As the implementation, the coordinator reads the commit offset
    of the followers, whatever their term, and compares offsets only.
  - The metadata update only checks that the term hasn't changed,
    as the implementation: another coordinator may have elected a
    leader since.
----------------------------------------------------------*)

IsQuorumCaughtUp(ostate) ==
    Cardinality({ n \in ostate.md.ensemble :
                    /\ n # ostate.election_leader
                    /\ node_state[n].commit_entry_id.offset >= ostate.election_leader_head.offset })
        >= Majority(Cardinality(ostate.md.ensemble)) - 1

CoordinatorFinishesEnsembleChange ==
    \* enabling conditions
    \E o \in Coordinators :
        LET ostate == coordinator_state[o]
        IN
            /\ ostate.status = RUNNING
            /\ ostate.election_phase = LEADER_ELECTED
            /\ ostate.md.change # NIL
            /\ IsQuorumCaughtUp(ostate)
            /\ metadata.term = ostate.md.term
            /\ metadata.change # NIL
            \* state changes
            /\ metadata' = FinishChange(metadata)
            /\ metadata_version' = metadata_version + 1
            /\ coordinator_state' = [coordinator_state EXCEPT ![o].md_version = metadata_version + 1,
                                                               ![o].md         = FinishChange(metadata)]
            /\ UNCHANGED << node_state, confirmed, messages, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Leader handles an Add Follower request

  The leader creates a cursor and will send a truncate
  request to the follower if their log might need
  truncating first.
----------------------------------------------------------*)

LeaderHandlesAddFollowerRequest ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, ADD_FOLLOWER_REQUEST)
        /\ node_state[msg.node].term = msg.term
        /\ node_state[msg.node].status = LEADER
        /\ node_state[msg.node].follow_cursor[msg.follower].status = NIL
        \* state changes
        /\ node_state' = [node_state EXCEPT ![msg.node].follow_cursor[msg.follower] =
                                GetCursor(node_state[msg.node], msg.head_entry_id)]
        /\ IF NeedsTruncation(node_state[msg.node], msg.head_entry_id)
           THEN ProcessedOneAndSendAnother(msg,
                                           GetTruncateRequest(node_state[msg.node],
                                                              msg.follower,
                                                              msg.head_entry_id.term))
           ELSE MessageProcessed(msg)
        /\ UNCHANGED << metadata, metadata_version, coordinator_state, confirmed, coordinator_stop_ctr >>


(*---------------------------------------------------------
  Node handles a Truncate request

  A node that receives a truncate request knows that it
  has been selected as a follower. It truncates its log
  to the indicates entry id, updates its term and changes
  to a Follower.

  The leader never truncates a committed entry, as it holds every
  committed entry: NoLossOfConfirmedWrite checks it. The
  implementation also refuses a truncation below the commit offset
  of the follower, as a guard.
----------------------------------------------------------*)

GetTruncateResponse(msg, head_entry_id) ==
    [type        |-> TRUNCATE_RESPONSE,
     dest_node   |-> msg.source_node,
     source_node |-> msg.dest_node,
     head_entry_id  |-> head_entry_id,
     term       |-> msg.term]

TruncateLog(log, last_safe_entry_id) ==
    { entry \in log : IsLowerOrEqualId(entry.entry_id, last_safe_entry_id) }

NodeHandlesTruncateRequest ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, TRUNCATE_REQUEST)
        /\ LET nstate        == node_state[msg.dest_node]
               truncated_log == TruncateLog(nstate.log, msg.head_entry_id)
               head_entry    == HeadEntry(truncated_log)
           IN
                /\ nstate.status = FENCED
                /\ nstate.term = msg.term
                \* state changes
                /\ node_state' = [node_state EXCEPT ![msg.dest_node] =
                                        [@ EXCEPT !.status        = FOLLOWER,
                                                  !.term         = msg.term,
                                                  !.leader        = msg.source_node,
                                                  !.log           = truncated_log,
                                                  !.head_entry_id    = head_entry.entry_id,
                                                  !.follow_cursor = [n \in Nodes |-> NoCursor]]]
                /\ ProcessedOneAndSendAnother(msg, GetTruncateResponse(msg, head_entry.entry_id))
                /\ UNCHANGED << metadata, metadata_version, coordinator_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  Leader handles a truncate response.

  The leader now activates the follow cursor as the follower
  log is now ready for replication.
----------------------------------------------------------*)

LeaderHandlesTruncateResponse ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, TRUNCATE_RESPONSE)
        /\ LET nstate == node_state[msg.dest_node]
           IN
                /\ nstate.status = LEADER
                /\ nstate.term = msg.term
                \* state changes
                /\ node_state' = [node_state EXCEPT ![msg.dest_node].follow_cursor =
                                        [@ EXCEPT ![msg.source_node] = [status         |-> ATTACHED,
                                                                        last_pushed    |-> msg.head_entry_id,
                                                                        last_confirmed |-> msg.head_entry_id]]]
                /\ MessageProcessed(msg)
                /\ UNCHANGED << metadata, metadata_version, coordinator_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  A client writes an entry to the leader

  A client writes a value from Values to a leader node
  if that value has not previously been written. The leader adds
  the entry to its log, updates its head_entry_id.
----------------------------------------------------------*)

Write ==
    \* enabling conditions
    \E n \in Nodes, v \in Values :
        LET nstate == node_state[n]
        IN
            /\ node_state[n].status = LEADER
            /\ v \notin DOMAIN confirmed
            \* state changes
            /\ LET entry_id  == [offset |-> nstate.head_entry_id.offset + 1,
                                 term  |-> nstate.term]
                   log_entry == [entry_id |-> entry_id,
                                 value    |-> v]
               IN
                    /\ node_state' = [node_state EXCEPT ![n] =
                                        [@ EXCEPT !.log        = @ \union { log_entry },
                                                  !.head_entry_id = entry_id]]
                    /\ confirmed' = confirmed @@ (v :> FALSE)
            /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, messages, coordinator_stop_ctr >>


(*---------------------------------------------------------
  A leader node sends entries to followers

  The leader will send an entry to a follower when it has
  entries in its log that is higher than the follow cursor
  position of that follower. In this action, the leader
  will send the next entry for all followers whose cursor is
  lower then the leader's head entry_id.

  This action chooses the largest subset of followers that
  have an attached cursor and whose cursor is behind the head
  of the leaders log. This is a state space reduction
  strategy as if we can send multiple requests at the same
  time we reduce the number of ways we send messages thus
  reducing the state space.
----------------------------------------------------------*)

CanSendToFollower(lstate, follower) ==
    /\ node_state[follower].status # NOT_MEMBER
    /\ follower # lstate.id
    /\ lstate.follow_cursor[follower].status = ATTACHED
    /\ IsHigherId(lstate.head_entry_id, lstate.follow_cursor[follower].last_pushed)

\* does this group of followers include all followers that we can send to in this moment?
IncludesAllSendableFollowers(lstate, followers) ==
    \* all followers in this group have their cursor behind the head of the leader log
    /\ \A f \in followers : CanSendToFollower(lstate, f)
    \* there isn't a follower who could be sent to, but is not in this group
    /\ ~\E f \in Nodes :
        /\ f # lstate.id
        /\ f \notin followers
        /\ CanSendToFollower(lstate, f)

\* Get the next lowest entry above the current follow cursor position
\* specifically, choose an entry that is higher than the last pushed
\* such that there is not another entry that is also higher than the
\* last pushed but lower than the chosen entry. Only one entry can
\* match that criteria.
NextEntry(lstate, last_entry_id) ==
    CHOOSE e1 \in lstate.log :
        /\ IsHigherId(e1.entry_id, last_entry_id)
        /\ ~\E e2 \in lstate.log :
            /\ IsLowerId(e2.entry_id, e1.entry_id)
            /\ IsHigherId(e2.entry_id, last_entry_id)

GetAppendEntry(lstate, next_entries, follower) ==
    [type            |-> APPEND,
     dest_node       |-> follower,
     source_node     |-> lstate.id,
     entry           |-> next_entries[follower],
     commit_entry_id |-> lstate.commit_entry_id,
     term            |-> lstate.term]

GetNextAppendEntries(lstate, next_entries, followers) ==
    { GetAppendEntry(lstate, next_entries, f) : f \in followers }

GetNextEntries(lstate, followers) ==
    [f \in followers |-> NextEntry(lstate, lstate.follow_cursor[f].last_pushed)]

LeaderSendsEntriesToFollowers ==
    \* enabling conditions
    \E leader \in Nodes, followers \in SUBSET Nodes :
        /\ followers # {}
        /\ LET lstate == node_state[leader]
           IN
            /\ lstate.status = LEADER
            /\ IncludesAllSendableFollowers(lstate, followers)
            \* state changes
            /\ LET next_entries == GetNextEntries(lstate, followers)
               IN /\ node_state' = [node_state EXCEPT ![leader].follow_cursor =
                                        [n \in Nodes |->
                                            IF n \in followers
                                            THEN [lstate.follow_cursor[n] EXCEPT !.last_pushed = next_entries[n].entry_id]
                                            ELSE lstate.follow_cursor[n]]]
                  /\ SendMessages(GetNextAppendEntries(lstate, next_entries, followers))
                  /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  A follower node confirms an entry to the leader

  The follower adds the entry to its log, sets the head entry_id
  and updates its commit entry_id with the commit entry_id of
  the request.
----------------------------------------------------------*)

GetAddEntryOkResponse(msg, fstate, entry) ==
    [type        |-> ACK,
     dest_node   |-> msg.source_node,
     source_node |-> fstate.id,
     code        |-> OK,
     entry_id    |-> entry.entry_id,
     term       |-> fstate.term]

FollowerConfirmsEntry ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ IsEarliestReceivableEntryMessage(messages, msg, APPEND)
        /\ LET f      == msg.dest_node
               fstate == node_state[f]
           IN
                /\ fstate.status \in {FOLLOWER, FENCED}
                /\ fstate.term <= msg.term \* reconfig could mean leader has higher term than follower
                \* state changes
                /\ node_state' = [node_state EXCEPT ![f].status       = FOLLOWER,
                                                    ![f].term        = msg.term,
                                                    ![f].leader       = msg.source_node,
                                                    ![f].log          = @ \union { msg.entry },
                                                    ![f].head_entry_id   = msg.entry.entry_id,
                                                    ![f].commit_entry_id = msg.commit_entry_id]

                /\ ProcessedOneAndSendAnother(msg, GetAddEntryOkResponse(msg, fstate, msg.entry))
                /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, confirmed, coordinator_stop_ctr >>


(*---------------------------------------------------------
  A follower node rejects an entry from the leader.


  If the leader has a lower term than the follower then the
  follower must reject it with an INVALID_TERM response.

  Key points:
  - The term of the response should be the term of the
    request so that the leader will not ignore the response.
----------------------------------------------------------*)

GetAddEntryInvalidTermResponse(nstate, msg) ==
    [type        |-> ACK,
     dest_node   |-> msg.source_node,
     source_node |-> nstate.id,
     code        |-> INVALID_TERM,
     entry_id    |-> msg.entry.entry_id,
     term       |-> msg.term]

FollowerRejectsEntry ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ IsEarliestReceivableEntryMessage(messages, msg, APPEND)
        /\ LET nstate == node_state[msg.dest_node]
           IN
                /\ nstate.term > msg.term
                \* state changes
                /\ ProcessedOneAndSendAnother(msg, GetAddEntryInvalidTermResponse(nstate, msg))
                /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, node_state, confirmed, coordinator_stop_ctr >>

(*---------------------------------------------------------
  A leader node handles an add entry response

  The leader updates the follow cursor last_confirmed
  entry id, it also may advance the commit entry_id.

  An entry is committed when all of the following
  has occurred:
  - it has reached majority quorum
  - the entire log prefix has reached majority quorum
  - the entry term matches the current term

  Key points:
  - Entries of prior terms cannot be committed directly by
    themselves, only entries of the current term can be
    committed. Committing an entry of the current term
    also implicitly includes all prior entries.
    To find a counterexample where loss of confirmed writes
    occurs when committing entries of prior terms directly,
    comment out the condition 'entry_id.term = nstate.term'
    below. Also see the Raft paper.

----------------------------------------------------------*)
GetEntry(entry_id, nstate) ==
    CHOOSE entry \in nstate.log :
        CompareLogEntries(entry.entry_id, entry_id) = 0

\* note that >= RF/2 instead of RF/2 + 1. This is because the
\* leader is counting follow cursors only. It already has the
\* entry and is the + 1.
EntryReachedQuorum(entry_id, follow_cursor, rep_factor) ==
    Cardinality({ n \in Nodes :
                    /\ follow_cursor[n].status = ATTACHED
                    /\ IsLowerOrEqualId(entry_id, follow_cursor[n].last_confirmed) })
                >= (rep_factor \div 2)

LogPrefixAtQuorum(nstate, entry_id, follow_cursor, rep_factor) ==
    \A entry \in nstate.log :
        \/ IsHigherId(entry.entry_id, entry_id)
        \/ /\ IsLowerOrEqualId(entry.entry_id, entry_id)
           /\ EntryReachedQuorum(entry.entry_id, follow_cursor, rep_factor)

EntryIsCommitted(nstate, entry_id, cursor, rep_factor) ==
    /\ LogPrefixAtQuorum(nstate, entry_id, cursor, rep_factor)
    /\ entry_id.term = nstate.term \* can only commit entries of the current term

LeaderHandlesEntryConfirm ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ IsEarliestReceivableEntryMessage(messages, msg, ACK)
        /\ LET leader   == msg.dest_node
               follower == msg.source_node
               nstate   == node_state[leader]
           IN
                /\ nstate.status = LEADER
                /\ nstate.term = msg.term
                /\ msg.code = OK
                /\ nstate.follow_cursor[follower].status = ATTACHED
                \* state changes
                /\ LET updated_cursor == [nstate.follow_cursor EXCEPT ![follower].last_confirmed = msg.entry_id]
                       entry          == GetEntry(msg.entry_id, nstate)
                   IN
                      /\ IF EntryIsCommitted(nstate, msg.entry_id, updated_cursor, nstate.rep_factor)
                         THEN /\ node_state' = [node_state EXCEPT ![leader].follow_cursor = updated_cursor,
                                                                  ![leader].commit_entry_id  = IF IsHigherId(msg.entry_id, @)
                                                                                            THEN msg.entry_id
                                                                                            ELSE @]
                              /\ confirmed' = [confirmed EXCEPT ![entry.value] = TRUE]
                         ELSE /\ node_state' = [node_state EXCEPT ![leader].follow_cursor = updated_cursor]
                              /\ UNCHANGED confirmed
                      /\ MessageProcessed(msg)
            /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, coordinator_stop_ctr >>

(*---------------------------------------------------------
  A leader node handles an add entry rejection response

  On receiving an INVALID_TERM response the leader knows
  it is stale and it abdicates by fencing itself.
----------------------------------------------------------*)
LeaderHandlesEntryRejection ==
    \* enabling conditions
    \E msg \in DOMAIN messages :
        /\ IsEarliestReceivableEntryMessage(messages, msg, ACK)
        /\ LET nstate == node_state[msg.dest_node]
           IN
                /\ nstate.status = LEADER
                /\ nstate.term = msg.term
                /\ msg.code = INVALID_TERM
                \* Actions
                /\ node_state' = [node_state EXCEPT ![msg.dest_node].status        = FENCED,
                                                    ![msg.dest_node].follow_cursor = [n1 \in Nodes |-> NoCursor]]
                /\ MessageProcessed(msg)
                /\ UNCHANGED << metadata, metadata_version, coordinator_state, metadata, confirmed, coordinator_stop_ctr >>


(*---------------------------------------------------------
  Next state formula
----------------------------------------------------------*)
Next ==
    \* Election
    \/ CoordinatorStarts                             \* an coordinator starts and loads the metadata
    \/ CoordinatorStops                              \* a running coordinator stops, losing all local state
    \/ CoordinatorStartsElection                     \* election starts, fencing requests sent
    \/ NodeHandlesFencingRequest                  \* node fences itself, responds with head entry_id
    \/ CoordinatorHandlesFencingResponse             \* coordinator handles fence response, during the fencing
    \/ CoordinatorElectsLeader                       \* fence reached quorum, coordinator chooses and persists the leader
    \/ NodeHandlesBecomeLeaderRequest             \* new leader receives notification of leadership
    \/ CoordinatorHandlesBecomeLeaderResponse        \* coordinator handles new leader confirmation
    \/ NodeHandlesTruncateRequest                 \* follower truncates it log to safe point
    \/ LeaderHandlesTruncateResponse              \* leader receives truncation response
    \/ CoordinatorHandlesPostQuorumFencingResponse   \* coordinator handles fence response after leader elected, adds as a follower
    \/ LeaderHandlesAddFollowerRequest            \* leader notified of follower (post election)
    \/ CoordinatorFinishesEnsembleChange          \* a majority of the ensemble caught up, the change is finished
    \* Writes and replication
    \/ Write                                      \* a client writes an entry to the leader
    \/ LeaderSendsEntriesToFollowers              \* leader sends entries to followers via cursors
    \/ FollowerConfirmsEntry                      \* follower receives an entry, confirms to leader
    \/ FollowerRejectsEntry                       \* follower receives an entry with a stale term, sends INVALID_TERM to leader
    \/ LeaderHandlesEntryConfirm                  \* leader receives add confirm, confirms to client if reached majority
    \/ LeaderHandlesEntryRejection                \* leader receives add rejection, abdicates

(*-------------------------------------------------
    INVARIANTS
--------------------------------------------------*)

AreEqual(entry1, entry2) ==
    /\ entry1.entry_id.term = entry2.entry_id.term
    /\ entry1.entry_id.offset = entry2.entry_id.offset
    /\ entry1.value = entry2.value


\* The log on all nodes matches at and below the commit entry_id
NoLogDivergence ==
    IF metadata.leader # NIL
    THEN \A n \in metadata.ensemble :
        \* all entries at or below commit entry_id contain the same value
        \* for the given entry_id across all nodes that have it
        /\ \A entry \in node_state[metadata.leader].log :
            IF IsLowerOrEqualId(entry.entry_id, node_state[n].commit_entry_id)
            THEN ~\E copy \in node_state[n].log :
                    /\ IsSameId(entry.entry_id, copy.entry_id)
                    /\ ~AreEqual(entry, copy)
            ELSE TRUE        
        \* all entries in the node log exists in the leader log
        /\ \A entry \in node_state[n].log :
            IF IsLowerOrEqualId(entry.entry_id, node_state[n].commit_entry_id)
            THEN \E match \in node_state[metadata.leader].log :
                AreEqual(entry, match)
            ELSE TRUE
    ELSE TRUE

\* There cannot be a confirmed write that does not exist in the leader's log
NoLossOfConfirmedWrite ==
    IF metadata.leader # NIL /\ Cardinality(DOMAIN confirmed) > 0 
    THEN
        \A v \in DOMAIN confirmed :
            \/ /\ confirmed[v] = TRUE
               /\ \E entry \in node_state[metadata.leader].log : entry.value = v
            \/ confirmed[v] = FALSE
    ELSE TRUE

\* messages should not contain conflicting data
ValidMessages ==
    /\ ~\E msg \in DOMAIN messages :
        /\ msg.type = ADD_FOLLOWER_REQUEST
        /\ msg.follower = msg.node
    /\ ~\E msg \in DOMAIN messages :
        /\ msg.type \in { APPEND, ACK,
                          TRUNCATE_REQUEST, TRUNCATE_RESPONSE }
        /\ msg.dest_node = msg.source_node

LegalLeaderAndEnsemble ==
    /\ IF metadata.leader # NIL
       THEN metadata.leader \in metadata.ensemble
       ELSE TRUE
    /\ \A o \in Coordinators :
        IF /\ coordinator_state[o].md # NIL
           /\ coordinator_state[o].md.leader # NIL
        THEN coordinator_state[o].md.leader \in coordinator_state[o].md.ensemble
        ELSE TRUE
    /\ metadata.rep_factor = Cardinality(metadata.ensemble)

\* A change of the ensemble swaps one member, the ensemble keeps its size
LegalEnsembleChange ==
    /\ Cardinality(metadata.ensemble) = RepFactor
    /\ IF metadata.change # NIL
       THEN /\ metadata.change.to \in metadata.ensemble
            /\ metadata.change.from \notin metadata.ensemble
       ELSE TRUE

(*-------------------------------------------------    
  LIVENESS
    
  Eventually:
  - all values will be committed.
--------------------------------------------------*)
AllValuesCommitted ==
    \A v \in Values :
        /\ v \in DOMAIN confirmed
        /\ confirmed[v] = TRUE

EventuallyAllValuesCommitted ==
    []<>AllValuesCommitted

LivenessSpec == Init /\ [][Next]_vars /\ WF_vars(Next)    

=============================================================================
