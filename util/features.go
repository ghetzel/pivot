package util

var features = make(map[string]bool)

func EnableFeature(name string) {
	features[name] = true
}

func DisableFeature(name string) {
	features[name] = false
}

func Features(names ...string) bool {
	for _, name := range names {
		if v, ok := features[name]; !ok || !v {
			return false
		}
	}

	return true
}

func AnyFeatures(names ...string) bool {
	for _, name := range names {
		if v, ok := features[name]; ok && v {
			return true
		}
	}

	return false
}
