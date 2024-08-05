type Storage struct {
	mutex            sync.Mutex
	data             map[string]string
	lastAppliedIndex int
}

func (st *Storage) ApplyCommand(index int, command Op) string {
	st.mutex.Lock()
	defer st.mutex.Unlock()

	if index <= st.lastAppliedIndex {
		return ""
	}

	st.lastAppliedIndex = index

	if command.Op == "Put" {
		switch command.SubOp {
		case "Append":
			st.putAppend(command.Key, command.Value)
		case "Put":
			st.put(command.Key, command.Value)
		}
	}

	return st.get(command.Key)
}

func (st *Storage) put(key, val string) {
	st.data[key] = val
}

func (st *Storage) putAppend(key, val string) {
	v, ok := st.data[key]
	if !ok {
		st.data[key] = val
	} else {
		st.data[key] = fmt.Sprintf("%s%s", val, v)
	}
}

func (st *Storage) get(key string) string {
	return st.data[key]
}