package ketama

import (
	"crypto/sha1"
	"log"
	"sort"
	"strconv"
)

type node struct {
	node string // node name
	hash uint
}

type tickArray []node // 桶，是区间。但是由于是环形队列，n个桶有n个区间

func (p tickArray) Len() int           { return len(p) }
func (p tickArray) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p tickArray) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p tickArray) Sort()              { sort.Sort(p) }

type hashRing struct {
	defaultSpots int       // 每台物理机模拟的虚拟机数量
	ticks        tickArray // 虚拟机
	length       int       // 虚拟机数量
}

func NewRing(n int) (h *hashRing) {
	log.SetFlags(log.Lshortfile)
	h = new(hashRing)
	h.defaultSpots = n
	return
}

// Adds a new node to a hash ring
// name: name of the server
// s: multiplier for default number of ticks (useful when one cache node has more resources, like RAM, than another)
// s 是倍数，如果某台机器配置好，可以适当增加倍数
func (h *hashRing) AddNode(name string, s int) {
	tSpots := h.defaultSpots * s
	hash := sha1.New()
	for i := 1; i <= tSpots; i++ {
		// 名称和虚拟编号求哈希
		hash.Write([]byte(name + ":" + strconv.Itoa(i)))
		// 20 字节。sha1是160位
		hashBytes := hash.Sum(nil)

		n := &node{
			node: name,
			// 取后4个字节求出哈希
			hash: uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24,
		}

		h.ticks = append(h.ticks, *n)
		hash.Reset()
	}
}

func (h *hashRing) DelNode(name string, s int) {
	tSpots := h.defaultSpots * s
	hash := sha1.New()
	for i := 1; i <= tSpots; i++ {
		// 名称和虚拟编号求哈希
		hash.Write([]byte(name + ":" + strconv.Itoa(i)))
		// 20 字节。sha1是160位
		hashBytes := hash.Sum(nil)

		v := uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24

		var search = func(x int) bool {
			return h.ticks[x].hash >= v
		}

		j := sort.Search(h.length, search)
		log.Println(h.length, j)
		h.ticks = append(h.ticks[:j], h.ticks[j+1:]...)
		h.length = len(h.ticks)

		hash.Reset()
	}
}

func (h *hashRing) Bake() {
	h.ticks.Sort()
	h.length = len(h.ticks)
}

func (h *hashRing) Hash(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	v := uint(hashBytes[19]) | uint(hashBytes[18])<<8 | uint(hashBytes[17])<<16 | uint(hashBytes[16])<<24
	// Search uses binary search to find and return the smallest index i in [0, n) at which f(i) is true, assuming that on the range [0, n), f(i) == true implies f(i+1) == true
	// 通过二分查找，找到比f(i)小的最大值
	i := sort.Search(h.length, func(i int) bool { return h.ticks[i].hash >= v })

	// 这是个环
	if i == h.length {
		i = 0
	}

	return h.ticks[i].node
}

func (h *hashRing) Debug() {
	for _, tick := range h.ticks {
		log.Println(tick.hash, tick.node)
	}
}

//////////// murmur hash ////////////

const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
	c3 = 0x85ebca6b
	c4 = 0xc2b2ae35
	r1 = 15
	r2 = 13
	m  = 5
	n  = 0xe6546b64
)

var (
	Seed = uint32(1)
)

func Murmur3(key []byte) (hash uint32) {
	hash = Seed
	iByte := 0
	for ; iByte+4 <= len(key); iByte += 4 {
		k := uint32(key[iByte]) | uint32(key[iByte+1])<<8 | uint32(key[iByte+2])<<16 | uint32(key[iByte+3])<<24
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2
		hash ^= k
		hash = (hash << r2) | (hash >> (32 - r2))
		hash = hash*m + n
	}

	var remainingBytes uint32
	switch len(key) - iByte {
	case 3:
		remainingBytes += uint32(key[iByte+2]) << 16
		fallthrough
	case 2:
		remainingBytes += uint32(key[iByte+1]) << 8
		fallthrough
	case 1:
		remainingBytes += uint32(key[iByte])
		remainingBytes *= c1
		remainingBytes = (remainingBytes << r1) | (remainingBytes >> (32 - r1))
		remainingBytes = remainingBytes * c2
		hash ^= remainingBytes
	}

	hash ^= uint32(len(key))
	hash ^= hash >> 16
	hash *= c3
	hash ^= hash >> 13
	hash *= c4
	hash ^= hash >> 16

	// 出发吧，狗嬷嬷！
	return
}
