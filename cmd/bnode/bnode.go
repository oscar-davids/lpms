package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"dbscan"

	"github.com/livepeer/lpms/ffmpeg"
)

type MultiPoint struct {
	index int
	pos   []float64
}
type Evidence struct {
	rendpath  string
	positions []float64
	lengths   []int
	features  []float64
}
type VerifyResult struct {
	bestpath string
	badpath  string
}

const PACKETSIZE int64 = 188

func (s MultiPoint) DistanceTo(c dbscan.Point, bl2 bool) float64 {
	distance := 0.0
	if bl2 == true {
		if len(c.(MultiPoint).pos) != len(s.pos) {
			distance = math.MaxFloat64
			return distance
		}

		for i := 0; i < len(c.(MultiPoint).pos); i++ {
			sl1 := c.(MultiPoint).pos[i] - s.pos[i]
			distance += sl1 * sl1
		}
	} else {
		dotproduct := 0.0
		s1 := 0.0
		s2 := 0.0
		if len(c.(MultiPoint).pos) != len(s.pos) {
			distance = 2.0
			return distance
		}

		for i := 0; i < len(c.(MultiPoint).pos); i++ {
			dotproduct += c.(MultiPoint).pos[i] * s.pos[i]
			s1 += s.pos[i] * s.pos[i]
			s2 += c.(MultiPoint).pos[i] * c.(MultiPoint).pos[i]
		}

		distance = dotproduct / (math.Sqrt(s1) * math.Sqrt(s2))
		distance = 1.0 - distance
	}
	//log.Println("distance:", 1.0-distance)
	return distance
}

func (s MultiPoint) Name() string {
	return fmt.Sprint(s.index)
}

func validRenditions() []string {
	valids := make([]string, len(ffmpeg.VideoProfileLookup))
	for p, _ := range ffmpeg.VideoProfileLookup {
		valids = append(valids, p)
	}
	return valids
}
func FilenameWithoutExtension(fn string) string {
	return strings.TrimSuffix(fn, path.Ext(fn))
}
func getids(field []string) (int, int, int, int) {
	idpath := 0
	idposition := 5
	idlength := 6
	idfeatures := 8
	for i, value := range field {
		if value == "outpath" {
			idpath = i
		}
		if value == "position" {
			idposition = i
		}
		if value == "length" {
			idlength = i
		}
		if value == "features" {
			idfeatures = i
		}
	}
	return idpath, idposition, idlength, idfeatures
}
func getfarray(recode string) (varray []float64) {
	s := strings.ReplaceAll(recode, "\"", "")
	s = strings.ReplaceAll(s, " ", "")
	sarray := strings.Split(s, ",")
	for _, i := range sarray {
		j, err := strconv.ParseFloat(i, 64)
		if err != nil {
			panic(err)
		}
		varray = append(varray, j)
	}
	return varray
}
func getiarray(recode string) (varray []int) {
	s := strings.ReplaceAll(recode, "\"", "")
	s = strings.ReplaceAll(s, " ", "")
	sarray := strings.Split(s, ",")

	for _, i := range sarray {
		j, err := strconv.Atoi(i)
		if err != nil {
			panic(err)
		}
		varray = append(varray, j)
	}
	return varray
}

func getcommonids(count int, clusters ...[][]dbscan.Point) (ids []int) {

	checkcount := len(clusters)
	for i := 1; i <= count; i++ {
		machnum := 0
		for _, cls := range clusters {
			for _, cl := range cls {
				for _, c := range cl {
					if c.(MultiPoint).index == i {
						machnum++
					}
				}
			}
		}
		if machnum == checkcount {
			ids = append(ids, i-1)
		}
	}
	return ids
}

func checkvalidation(fname string, pkpos []int64, pklength []int64) int {

	f, err := os.Open(fname)
	defer f.Close()
	if err != nil {
		return -1
	}

	ret := 0
	pkt := make([]byte, PACKETSIZE)
	count := len(pkpos)

	for i := 0; i < count; i++ {
		vcount := 0
		packet_pid := 0
		pkcount := int(pklength[i] / PACKETSIZE)
		_, err := f.Seek(pkpos[i], 0)
		if err != nil {
			return -1
		}

		for j := 0; j < pkcount; j++ {
			_, err := f.Read(pkt)
			if err != nil {
				return -1
			}

			if pkt[0] != 0x47 {
				ret = -1
				break
			}

			packet_pid = int((pkt[1]&0x1F))<<8 | int((pkt[2] & 0xFF))
			if packet_pid != 256 {
				continue
			}
			//pes check
			payload := make([]byte, 0)
			if (pkt[3]>>4)&3 > 0 {
				payload = pkt[4 : PACKETSIZE-1]
			}
			if (pkt[3]>>5)&1 > 0 {
				payload = pkt[5+pkt[4] : PACKETSIZE-1]
			}
			if (pkt[1]>>6)&1 > 0 && len(payload) > 0 {
				/* Validate the start code */
				if payload[0] != 0x00 || payload[1] != 0x00 || payload[2] != 0x01 {
					ret = -1
					break
				}
				if payload[3] >= 0xE0 && payload[3] <= 0xEF {
					vcount++
				}
				if payload[3] >= 0xC0 && payload[3] <= 0xDF {
					//ret = -1
					vcount++
					//break
				}
			}
		}
		if vcount != 1 || ret == -1 {
			//fmt.Println("break--", vcount, ret, packet_pid)
			ret = -1
			break
		}
	}

	return ret
}

func Verify(evidences []Evidence, flagdiffcos bool, flagdiffl2 bool, useposvec bool, lencheck bool) string {
	bestrendpath := ""
	retids := make([]int, 0, 0)
	positionvec := make([]dbscan.Point, 0, 0)
	featurevec := make([]dbscan.Point, 0, 0)
	for i, e := range evidences {
		positionvec = append(positionvec, MultiPoint{index: i + 1, pos: e.positions})
		featurevec = append(featurevec, MultiPoint{index: i + 1, pos: e.features})
	}
	//check skip encode
	clusterlist := make([][][]dbscan.Point, 0, 0)

	if flagdiffcos == true {
		clustersft := dbscan.Cluster(2, 0.00001, false, featurevec...)
		clusterlist = append(clusterlist, clustersft)
	}
	if flagdiffl2 == true {
		//check encoding attack such as watermark, flip
		clustersother := dbscan.Cluster(2, 1200000000, true, featurevec...)
		clusterlist = append(clusterlist, clustersother)
	}
	if useposvec == true {
		clusterspos := dbscan.Cluster(2, 0.0015, false, positionvec...)
		clusterlist = append(clusterlist, clusterspos)
	}

	ids := getcommonids(len(evidences), clusterlist...)

	if lencheck == true {
		//select first received rendition
		for _, id := range ids {
			positions := make([]int64, 0, 0)
			lenghts := make([]int64, 0, 0)
			for i := 0; i < len(evidences[id].positions); i++ {
				positions = append(positions, int64(evidences[id].positions[i]))
				lenghts = append(lenghts, int64(evidences[id].lengths[i]))
			}
			ret := checkvalidation(evidences[id].rendpath, positions, lenghts)
			if ret == 0 {
				retids = append(retids, id)
				break
			} /*else { //for debug
				fmt.Println("packet fail rendition", evidences[id].rendpath)
			}*/
		}
	} else {
		retids = ids
	}

	if len(retids) > 0 {
		bestrendpath = evidences[retids[0]].rendpath
	}

	return bestrendpath
}

func main() {

	if len(os.Args) != 7 {
		panic("Usage: <csv files directory> <rendition directory> <diffcos> <diffl2> <poscheck> <lengthcheck>")
	}

	fcsvdir := os.Args[1]
	renddir := os.Args[2]
	flagdiffcos, _ := strconv.ParseBool(os.Args[3])
	flagdiffl2, _ := strconv.ParseBool(os.Args[4])
	flagpos, _ := strconv.ParseBool(os.Args[5])
	flaglength, _ := strconv.ParseBool(os.Args[6])

	files, err := ioutil.ReadDir(fcsvdir)
	if err != nil {
		log.Fatal(err)
	}

	var csvfiles []*os.File
	var csvreaders []*csv.Reader
	var flist []string

	for _, f := range files {
		fpath := fcsvdir + "/" + f.Name()
		if strings.Contains(fpath, ".csv") == true {
			fmt.Println(fpath)
			csvf, err := os.Open(fpath)
			if err != nil {
				panic("can not read a csv file!")
			}
			csvfiles = append(csvfiles, csvf)
			csvr := csv.NewReader(csvf)
			csvreaders = append(csvreaders, csvr)
			defer csvf.Close()
			flist = append(flist, fpath)
		}
	}

	//bench mark
	starttime := time.Now()
	loopcount := 0
	correctcount := 0
	stopflag := false
	var idpath, idposition, idlength, idfeatures int
	for {
		evidences := make([]Evidence, 0, 0)
		for i, r := range csvreaders {
			record, err := r.Read()
			if err == io.EOF || err != nil {
				stopflag = true
				break
			}
			if loopcount == 0 { //first fields
				if i == 0 {
					idpath, idposition, idlength, idfeatures = getids(record)
				} else {
					continue
				}

			} else {
				s := strings.ReplaceAll(record[idpath], "\"", "")
				s = strings.ReplaceAll(s, " ", "")
				rendpath := renddir + "/" + s
				//rendpath := renddir + "/" + record[idpath]
				positions := getfarray(record[idposition])
				lengths := getiarray(record[idlength])
				features := getfarray(record[idfeatures])
				evidence := Evidence{rendpath: rendpath, positions: positions, lengths: lengths, features: features}
				evidences = append(evidences, evidence)
			}
		}

		if len(evidences) > 0 {
			selpath := Verify(evidences, flagdiffcos, flagdiffl2, flagpos, flaglength)
			if len(selpath) > 0 {
				correctcount++
			}
			//debug
			fmt.Println(selpath)
		}

		if stopflag == true {
			break
		}

		loopcount++
	}

	fmt.Println("loop count :", loopcount)
	fmt.Println("correct count :", correctcount)
	fmt.Println("accuracy :", float64(correctcount)/float64(loopcount-1))
	//fmt.Println("fpr :", float64(fprcount)/float64(loopcount-1))

	endtime := time.Now()
	elapsed := endtime.Sub(starttime)
	fmt.Println("starttime is: ", starttime.String())
	fmt.Println("end time is: ", endtime.String())
	fmt.Println("elapsed time is: ", elapsed.String())

	for i, f := range flist {
		fmt.Println("fileid:", i, f)
	}
}
