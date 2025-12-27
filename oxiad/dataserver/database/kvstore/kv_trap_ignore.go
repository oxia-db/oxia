//go:build disable_trap

package kvstore

type KvTrap struct {
	hooks map[string]func() error
}

func (trap *KvTrap) Trigger(string) error {
	return nil
}
