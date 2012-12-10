package radix

func (self *node) each(prefix []Nibble, use int, f nodeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		if !self.empty && (use == 0 || self.use&use != 0) {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
		}
		if cont {
			for _, child := range self.children {
				cont = child.each(prefix, use, f)
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEach(prefix []Nibble, use int, f nodeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		for i := len(self.children) - 1; i >= 0; i-- {
			cont = self.children[i].reverseEach(prefix, use, f)
			if !cont {
				break
			}
		}
		if cont {
			if !self.empty && (use == 0 || self.use&use != 0) {
				cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
			}
		}
	}
	return
}
func (self *node) eachBetween(prefix, min, max []Nibble, mincmp, maxcmp, use int, f nodeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if !self.empty && (use == 0 || self.use&use != 0) && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
		cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
	}
	if cont {
		for _, child := range self.children {
			if child != nil {
				childKey := make([]Nibble, len(prefix)+len(child.segment))
				copy(childKey, prefix)
				copy(childKey[len(prefix):], child.segment)
				m := len(childKey)
				if m > len(min) {
					m = len(min)
				}
				if m > len(max) {
					m = len(max)
				}
				if (min == nil || nComp(childKey[:m], min[:m]) > -1) && (max == nil || nComp(childKey[:m], max[:m]) < 1) {
					cont = child.eachBetween(prefix, min, max, mincmp, maxcmp, use, f)
				}
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEachBetween(prefix, min, max []Nibble, mincmp, maxcmp, use int, f nodeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	var child *node
	for i := len(self.children) - 1; i >= 0; i-- {
		child = self.children[i]
		if child != nil {
			childKey := make([]Nibble, len(prefix)+len(child.segment))
			copy(childKey, prefix)
			copy(childKey[len(prefix):], child.segment)
			m := len(childKey)
			if m > len(min) {
				m = len(min)
			}
			if m > len(max) {
				m = len(max)
			}
			if (min == nil || nComp(childKey[:m], min[:m]) > -1) && (max == nil || nComp(childKey[:m], max[:m]) < 1) {
				cont = child.reverseEachBetween(prefix, min, max, mincmp, maxcmp, use, f)
			}
			if !cont {
				break
			}
		}
	}
	if cont {
		if !self.empty && (use == 0 || self.use&use != 0) && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
		}
	}
	return
}

func (self *node) eachBetweenIndex(prefix []Nibble, count int, min, max *int, use int, f nodeIndexIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if !self.empty && (use == 0 || self.use&use != 0) && (min == nil || count >= *min) && (max == nil || count <= *max) {
		cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp, count)
		if use == 0 || self.use&use&byteValue != 0 {
			count++
		}
		if use == 0 || self.use&use&treeValue != 0 {
			count += self.treeValue.Size()
		}
	}
	if cont {
		relevantChildSize := 0
		for _, child := range self.children {
			if child != nil {
				relevantChildSize = 0
				if use == 0 || use&byteValue != 0 {
					relevantChildSize += child.byteSize
				}
				if use == 0 || use&treeValue != 0 {
					relevantChildSize += child.treeSize
				}
				if (min == nil || relevantChildSize+count > *min) && (max == nil || count <= *max) {
					cont = child.eachBetweenIndex(prefix, count, min, max, use, f)
				}
				count += relevantChildSize
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEachBetweenIndex(prefix []Nibble, count int, min, max *int, use int, f nodeIndexIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	var child *node
	relevantChildSize := 0
	for i := len(self.children) - 1; i >= 0; i-- {
		child = self.children[i]
		if child != nil {
			relevantChildSize = 0
			if use == 0 || use&byteValue != 0 {
				relevantChildSize += child.byteSize
			}
			if use == 0 || use&treeValue != 0 {
				relevantChildSize += child.treeSize
			}
			if (min == nil || relevantChildSize+count > *min) && (max == nil || count <= *max) {
				cont = child.reverseEachBetweenIndex(prefix, count, min, max, use, f)
			}
			count += relevantChildSize
			if !cont {
				break
			}
		}
	}
	if cont {
		if !self.empty && (use == 0 || self.use&use != 0) && (min == nil || count >= *min) && (max == nil || count <= *max) {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp, count)
			if use == 0 || self.use&use&byteValue != 0 {
				count++
			}
			if use == 0 || self.use&use&treeValue != 0 {
				count += self.treeValue.Size()
			}
		}
	}
	return
}
