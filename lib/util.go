package srm

type Joins struct {
	joinList []string
	onList []string
}

func (o *Joins)Size() int {
	return len(o.joinList)
}

func (o *Joins)Join(i int) string {
	return o.joinList[i]
}

func (o *Joins)On(i int) string {
	return o.onList[i]
}

func (o *Joins)Ij(on string) *Joins {
	o.init()
	o.joinList = append(o.joinList, "join")
	o.onList = append(o.onList, on)
	return o
}

func (o *Joins)Loj(on string) *Joins {
	o.init()
	o.joinList = append(o.joinList, "left outer join")
	o.onList = append(o.onList, on)
	return o
}

func (o *Joins) init() {
	if o.joinList == nil {
		o.joinList = make([]string, 0)
	}
	if o.onList == nil {
		o.onList = make([]string, 0)
	}
}


func Loj(on string) *Joins {
	j := Joins{}
	return j.Loj(on)
}

func Ij(on string) *Joins {
	j := Joins{}
	return j.Ij(on)
}
