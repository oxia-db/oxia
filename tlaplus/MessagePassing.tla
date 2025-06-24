--------------------------- MODULE MessagePassing ---------------------------
EXTENDS FiniteSets, Sequences, Integers, TLC

\* message types
CONSTANTS NEW_TERM_REQUEST,
          NEW_TERM_RESPONSE,
          BECOME_LEADER_REQUEST,
          BECOME_LEADER_RESPONSE,
          ADD_FOLLOWER_REQUEST,
          APPEND,
          ACK,
          TRUNCATE_REQUEST,
          TRUNCATE_RESPONSE

VARIABLES messages

(***************************************************************************)
(* Message Passing                                                         *)
(*                                                                         *)
(* Messages are represented by a funcion of MSG -> Delivery Count.         *)
(* Resending messages is not currently modelled but is supported by simply *)
(* incrementing the delivery count.                                        *)
(*                                                                         *)
(* NOTE: There are no ordering guarantees of message passing by default    *)
(***************************************************************************)

\* Send a set of messages only if none have been previously sent
\* In any given step, a random subset of these messages are lost (including none)
\* The TLA+ is simply choosing a delivery count for each message that
\* TLC will explore exhaustively.
SendMessages(msgs) ==
    /\ \A msg \in msgs : msg \notin DOMAIN messages
    /\ LET msgs_to_send == [m \in msgs |-> 1]
       IN messages' = messages @@ msgs_to_send
    
\* Send a message only if the message has not already been sent
SendMessage(msg) ==
    /\ msg \notin DOMAIN messages
    /\ messages' = messages @@ (msg :> 1)

\* Mark one message as processed and send a new message
ProcessedOneAndSendAnother(received_msg, send_msg) ==
    /\ received_msg \in DOMAIN messages
    /\ send_msg \notin DOMAIN messages
    /\ messages[received_msg] >= 1
    /\ messages' = [messages EXCEPT ![received_msg] = @-1] @@ (send_msg :> 1)

ProcessedOneAndSendMore(received_msg, send_msgs) ==
    /\ \A msg \in send_msgs : msg \notin DOMAIN messages
    /\ messages' = [messages EXCEPT ![received_msg] = @-1] @@ [m \in send_msgs |-> 1]

\* Mark one message as processed
MessageProcessed(msg) ==
    /\ msg \in DOMAIN messages
    /\ messages[msg] >= 1
    /\ messages' = [messages EXCEPT ![msg] = @ - 1]

\* The message is of this type and has been delivered to the recipient
ReceivableMessageOfType(msgs, msg, message_type) ==
    /\ msg.type = message_type
    /\ msgs[msg] >= 1
    
IsLower(log_entry1, log_entry2) ==
    \/ log_entry1.term < log_entry2.term
    \/ /\ log_entry1.term = log_entry2.term
       /\ log_entry1.offset < log_entry2.offset

IsEarliestReceivableEntryMessage(msgs, msg, type) ==
    /\ ReceivableMessageOfType(msgs, msg, type)
    /\ ~\E msg2 \in DOMAIN messages :
        /\ ReceivableMessageOfType(msgs, msg2, type)
        /\ msg2 # msg
        /\ \/ /\ msg.type = APPEND
              /\ IsLower(msg2.entry.entry_id, msg.entry.entry_id)
           \/ /\ msg.type = ACK
              /\ IsLower(msg2.entry_id, msg.entry_id)
        /\ msg2.dest_node = msg.dest_node
        /\ msg2.source_node = msg.source_node
        
CoordinatorMessagesLost(o) ==
    messages' = [msg \in DOMAIN messages |->
                    IF /\ msg.type \in {BECOME_LEADER_REQUEST,
                                        BECOME_LEADER_RESPONSE}
                       /\ msg.coordinator = o
                    THEN 0
                    ELSE messages[msg]]

=============================================================================
\* Modification History
\* Last modified Tue Jan 18 15:14:31 CET 2022 by jvanlightly
\* CreatedMon Dec 27 19:10:12 CET 2021 by jvanlightly