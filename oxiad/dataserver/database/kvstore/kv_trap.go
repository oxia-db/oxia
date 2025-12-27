//go:build !disable_trap

package kvstore

type KvTrap struct {
	hooks map[string]func() error
}

func NewKvTrap(hooks map[string]func() error) *KvTrap {
	return &KvTrap{
		hooks: hooks,
	}
}

func (trap *KvTrap) Trigger(name string) error {
	if trap == nil {
		return nil
	}
	if h := trap.hooks[name]; h != nil {
		return h()
	}
	return nil
}
