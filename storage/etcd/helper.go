package etcd

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"regexp"
	"sort"
	"strings"
)

const (
	labels   = "labels"
	topology = "topology"
	regions  = "regions"

	flush = "flush"

	AT_ONCE        = "atOnce"
	ROLLING_UPDATE = "rollingUpdate"
	CANARY         = "canary"
)

func Labels(lbs map[string]string) []string {
	lbls := []string{}
	for k, v := range lbs {
		kv := strings.Join([]string{k, v}, ":")
		lbls = append(lbls, kv)
	}
	sort.Strings(lbls)
	return lbls
}

// construct variable path
func JoinParts(artifact string, parts ...string) string {
	s := []string{}
	for _, part := range parts {
		s = append(s, part)
	}

	if artifact != "" {
		s = append(s, artifact)
	}
	return strings.Join(s, "/")
}

func SearchKey(regionid, clusterid string) (string, error) {
	if regionid == "*" && clusterid == "*" {
		return JoinParts("", topology, regions, labels), nil // topology/regions/labels/
	} else if regionid != "*" && clusterid == "*" {
		return JoinParts("", topology, regions, labels, regionid), nil // topology/regions/labels/regionid/
	} else if regionid != "*" && clusterid != "*" { //topology/regions/labels/regionid/clusterid/
		return JoinParts("", topology, regions, labels, regionid, clusterid), nil
	}
	return "", errors.New("Request not valid")
}

func Compare(a, b []string, strict bool) bool {
	for _, akv := range a {
		for _, bkv := range b {
			if akv == bkv && !strict {
				return true
			}
		}
	}
	return true
}

func SplitLabels(value string) []string {
	ls := strings.Split(value, ",")
	sort.Strings(ls)
	return ls
}

// topology/regionid/clusterid/nodeid
func Join(keyPart, artifact string) string {
	s := []string{keyPart, artifact}
	return strings.Join(s, "/")
}

//topology/regions/userId:namespace/labels/regionid/clusterid/
func NewKey(path, artifact string) string {
	keyPart := strings.Join(strings.Split(path, "/labels/"), "/")
	newKey := Join(keyPart, artifact)
	if !strings.Contains(newKey, ":topology") {
		var re = regexp.MustCompile("([a-zA-Z0-9])+:([a-zA-Z0-9])+/")
		s := re.ReplaceAllString(newKey, "")
		return Join(s, artifact)
	}
	return newKey
}

// topology/regions/regionid/clusterid/nodeid -> topology.regions.regionid.clusterid.nodeid
func TransformKey(oldKey string) string {
	noLabels := NewKey(oldKey, "")
	if strings.Contains(noLabels, ":topology") {
		parts := strings.Split(noLabels, "/")
		return parts[len(parts)-3]
	}

	temp := strings.Replace(noLabels, "/", ".", -1)
	return strings.TrimSuffix(temp, ".")
}

func UUID() string {
	u := uuid.Must(uuid.NewRandom())
	return fmt.Sprintf("%s", u)
}

func FlushKey(name, kind string) string {
	s := strings.Join([]string{name, UUID()}, "_")
	s1 := []string{flush, strings.ToLower(kind), s}
	return strings.Join(s1, "/")
}

func FlushTaskKey(name string) string {
	s := strings.Join([]string{name, UUID()}, "_")
	s1 := []string{flush, s}
	return strings.Join(s1, "/")
}

func Split(devider int, logs []string) [][]string {
	var divided [][]string
	chunkSize := (len(logs) + devider - 1) / devider
	for i := 0; i < len(logs); i += chunkSize {
		end := i + chunkSize
		if end > len(logs) {
			end = len(logs)
		}
		divided = append(divided, logs[i:end])
	}
	return divided
}

func Percent(pcent int, all int) int {
	percent := ((float64(all) * float64(pcent)) / float64(100))
	return int(percent)
}

func PercentSplit(x []string, percentages []float32) (error, [][]string) {
	psum := float32(0)
	for _, val := range percentages {
		psum += val
	}
	if psum != 1.0 {
		return errors.New("Percentages sum not 1"), nil
	}

	prv := 0
	size := float32(len(x))
	cum_percentage := float32(0)
	rez := [][]string{}
	for _, p := range percentages {
		cum_percentage += p
		nxt := int(cum_percentage * size)
		rez = append(rez, x[prv:nxt])
		prv = nxt
	}
	return nil, rez
}

func Kind(node string) string {
	parts := strings.Split(node, "/")
	return parts[len(parts)-1]
}

func GKind(node string) string {
	newNode := node
	if !strings.Contains(node, ":topology") {
		var re = regexp.MustCompile("/([a-zA-Z0-9])+:([a-zA-Z0-9])+/")
		newNode = re.ReplaceAllString(node, "/")
	}

	fmt.Println("KIND KEY:", newNode)
	parts := strings.Split(newNode, "/")
	if len(parts) > 6 {
		return parts[len(parts)-2]
	} else {
		return parts[len(parts)-1]
	}
}
