package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"dbscan"

	"github.com/golang/glog"
	"github.com/livepeer/lpms/ffmpeg"
)

type MultiPoint struct {
	index int
	pos   []float64
}
type Evidence struct {
	srcpath   string
	rendpath  string
	positions []float64
	lengths   []int
	features  []float64
	profile   ffmpeg.VideoProfile
}
type VerifyResult struct {
	bestpath string
	badpath  string
}

type Retryable struct {
	error
}

type Fatal struct {
	Retryable
}

const PACKETSIZE int64 = 188

var ErrMissingSource = errors.New("MissingSource")
var ErrVerifierStatus = errors.New("VerifierStatus")
var ErrVideoUnavailable = errors.New("VideoUnavailable")
var ErrTampered = Retryable{errors.New("Tampered")}

var ErrPosition = errors.New("BadPosition")
var ErrPkcheck = errors.New("ErrorPacket")
var ErrFeature = errors.New("BadFeatures")
var ErrInference = errors.New("ErrorInference")

type epicResolution struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}
type epicRendition struct {
	URI        string         `json:"uri"`
	Resolution epicResolution `json:"resolution"`
	Framerate  uint           `json:"frame_rate"`
	Pixels     int64          `json:"pixels"`
	Features   []float64      `json:"features"`
}
type epicRequest struct {
	Source         string          `json:"source"`
	Renditions     []epicRendition `json:"renditions"`
	OrchestratorID string          `json:"orchestratorID"`
}

type epicResultFields struct {
	VideoAvailable bool    `json:"video_available"`
	AudioAvailable bool    `json:"audio_available"`
	AudioDistance  float64 `json:"audio_dist"`
	Pixels         int64   `json:"pixels"`
	// 1 if the model detects a tamper and 0 otherwise
	Tamper int `json:"tamper"`
	// This value is used to order multiple results that are all marked as tampered which might indicate a misclassification
	// In this case, the result with the highest distance is considered the most preferable
	OCSVMDist float64 `json:"ocsvm_dist"`
}

type epicResults struct {
	Source  string             `json:"source"`
	Results []epicResultFields `json:"results"`
}

type EpicClassifier struct {
	Addr string
}

type Results struct {
	// Verifier specific score
	Tamper []int64
	Score  float64

	// Number of pixels decoded in this result
	Pixels []int64
}

func epicResultsToVerificationResults(er *epicResults) (*Results, error) {
	// find average of scores and build list of pixels
	var (
		tamper []int64
		pixels []int64
	)
	var err error
	// If an error is gathered, continue to gather overall pixel counts
	// In case this is a false positive. Only return the first error.
	for _, v := range er.Results {
		// The order of error checking is somewhat arbitrary for now
		// But generally it should check for fatal errors first, then retryable
		if v.Tamper > 0 {
			tamper = append(tamper, 1)
		} else {
			tamper = append(tamper, 0)
		}

	}
	return &Results{Tamper: tamper, Pixels: pixels}, err
}

func (e *EpicClassifier) Inference(evidences []Evidence) (*Results, error) {

	// Build the request object
	renditions := []epicRendition{}
	for _, v := range evidences {
		w, h, err := ffmpeg.VideoProfileResolution(v.profile)
		if err != nil {
			return nil, err
		}
		pixel := w * h
		r := epicRendition{
			URI:        v.rendpath,
			Resolution: epicResolution{Width: w, Height: h},
			Framerate:  v.profile.Framerate,
			Pixels:     int64(pixel),
			Features:   v.features,
		}
		renditions = append(renditions, r)
	}

	oid := "foo"
	req := epicRequest{
		Source:         evidences[0].srcpath,
		Renditions:     renditions,
		OrchestratorID: oid,
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		glog.Error("Could not marshal JSON for verifier! ", err)
		return nil, err
	}

	// Submit request and process results
	resp, err := http.Post(e.Addr, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		glog.Error("Could not submit request ", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return nil, ErrVerifierStatus
	}
	var er epicResults
	err = json.Unmarshal(body, &er)
	if err != nil {
		return nil, err
	}
	vr, err := epicResultsToVerificationResults(&er)
	return vr, err
}

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
func getids(field []string) (int, int, int, int, int) {
	idpath := 0
	idposition := 5
	idlength := 6
	idfeatures := 8
	idprofile := 5
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
		if value == "profile" {
			idprofile = i
		}
	}
	return idpath, idposition, idlength, idfeatures, idprofile
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

func getcommonids(count int, clusters ...[]dbscan.Point) (ids []int) {

	checkcount := len(clusters)

	for i := 1; i <= count; i++ {
		machnum := 0
		for _, cl := range clusters {
			for _, c := range cl {
				if c.(MultiPoint).index == i {
					machnum++
				}
			}
		}
		if machnum == checkcount {
			ids = append(ids, i-1)
		}
	}
	return ids
}
func getcommonids1(count int, clusters ...[][]dbscan.Point) (ids []int) {

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
		if pklength[i] == 0 && pkpos[i] == 0 {
			continue
		}
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
			if packet_pid == 257 {
				continue
			} else {
				//debug
				fmt.Println("break--", vcount, ret, packet_pid)
				ret = -1
				break
			}

		}
	}

	return ret
}

func Verify(evidences []Evidence, flagdiffcos bool, flaginference bool, flagpos bool, flaglength bool) (string, error) {
	bestrendpath := ""
	retids := make([]int, 0, 0)
	positionvec := make([]dbscan.Point, 0, 0)
	featurevec := make([]dbscan.Point, 0, 0)
	for i, e := range evidences {
		positionvec = append(positionvec, MultiPoint{index: i + 1, pos: e.positions})
		featurevec = append(featurevec, MultiPoint{index: i + 1, pos: e.features})
	}

	//check skip calculation diff feature and zero attack
	clusterlist := make([][]dbscan.Point, 0, 0)
	if flagdiffcos == true {
		clustersft := dbscan.Cluster(2, 0.00001, false, featurevec...)
		//debug
		if len(clustersft) > 0 {
			clusterlist = append(clusterlist, clustersft[0])
		} else {
			err := ErrPosition
			return bestrendpath, err
		}

	}
	//check skip encode
	if flagpos == true {
		//clusterspos := dbscan.Cluster(2, 0.0015, false, positionvec...)
		clusterspos := dbscan.Cluster(2, 0.0015, false, positionvec...)
		if len(clusterspos) > 0 {
			clusterlist = append(clusterlist, clusterspos[0])
		} else {
			err := ErrFeature
			return bestrendpath, err
		}
	}

	ids := getcommonids(len(evidences), clusterlist...)
	if flaginference == true {
		//check encoding attack such as watermark, flip
		//clustersother := dbscan.Cluster(2, 500000000000, true, featurevec...)
		//clusterlist = append(clusterlist, clustersother)

		ec := &EpicClassifier{Addr: "http://127.0.0.1:5000" + "/verify"}
		infresult, err := ec.Inference(evidences)
		//debug
		var tmpids []int
		if err == nil {
			for _, id := range ids {
				if infresult.Tamper[id] == 0 {
					tmpids = append(tmpids, id)
				}
			}
		} else {
			return bestrendpath, ErrInference
		}
		//interset commonid & inference result
		ids = tmpids
	}

	if flaglength == true {
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
			}
			if len(retids) == 0 {
				return bestrendpath, ErrPkcheck
			}
		}
	} else {
		retids = ids
	}

	if len(retids) > 0 {
		bestrendpath = evidences[retids[0]].rendpath
	}

	return bestrendpath, nil
}

func testvalidity(incsv string, renddir string, flagbreak bool) {

	csvf, err := os.Open(incsv)
	if err != nil {
		panic("can not read a csv file!")
	}
	csvr := csv.NewReader(csvf)
	defer csvf.Close()
	loopcount := 0
	failcount := 0
	var idpath, idposition, idlength int
	for {
		record, err := csvr.Read()
		if err == io.EOF || err != nil {
			break
		}
		if loopcount == 0 { //first fields
			idpath, idposition, idlength, _, _ = getids(record)

		} else {
			s := strings.ReplaceAll(record[idpath], "\"", "")
			s = strings.ReplaceAll(s, " ", "")
			rendpath := renddir + "/" + s
			//rendpath := renddir + "/" + record[idpath]
			fpositions := getfarray(record[idposition])
			flengths := getiarray(record[idlength])
			positions := make([]int64, 0, 0)
			lenghts := make([]int64, 0, 0)
			for i := 0; i < len(fpositions); i++ {
				positions = append(positions, int64(fpositions[i]))
				lenghts = append(lenghts, int64(flengths[i]))
			}
			ret := checkvalidation(rendpath, positions, lenghts)
			if ret != 0 {
				fmt.Println("position test fail: ", rendpath)
				failcount++
				if flagbreak {
					break
				}
			}
		}
		loopcount++
	}

	fmt.Println("total: ", loopcount, "fail: ", failcount)
}

func main() {

	fmt.Println("Start Bnode Benchmarking!")
	if len(os.Args) == 4 {
		//benchmark packet validity
		//csvfile, rend path, debug break(0, 1)
		flagbreak, _ := strconv.ParseBool(os.Args[3])
		starttime := time.Now()
		testvalidity(os.Args[1], os.Args[2], flagbreak)
		endtime := time.Now()
		elapsed := endtime.Sub(starttime)
		fmt.Println("starttime is: ", starttime.String())
		fmt.Println("end time is: ", endtime.String())
		fmt.Println("elapsed time is: ", elapsed.String())
		return
	}

	if len(os.Args) != 8 {
		panic("Usage: <csv files directory> <rendition directory> <diffcos> <inference> <poscheck> <lengthcheck> <loopcount>")
	}

	fcsvdir := os.Args[1]
	renddir := os.Args[2]
	flagdiffcos, _ := strconv.ParseBool(os.Args[3])
	flaginference, _ := strconv.ParseBool(os.Args[4])
	flagpos, _ := strconv.ParseBool(os.Args[5])
	flaglength, _ := strconv.ParseBool(os.Args[6])
	flagbreaknum, _ := strconv.Atoi(os.Args[7])

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
	var idpath, idposition, idlength, idfeatures, idprofile int
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
					idpath, idposition, idlength, idfeatures, idprofile = getids(record)
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
				evidence := Evidence{rendpath: rendpath, positions: positions, lengths: lengths,
					features: features, profile: ffmpeg.VideoProfileLookup[record[idprofile]]}
				evidences = append(evidences, evidence)
			}
		}

		if len(evidences) > 0 {
			selpath, _ := Verify(evidences, flagdiffcos, flaginference, flagpos, flaglength)
			if len(selpath) > 0 {
				correctcount++
			} else {
				//debug
				fmt.Println("fail id:", loopcount, evidences[0].rendpath)
				//stopflag = true
			}
			//debug
			//fmt.Println(selpath)
		}

		if stopflag == true {
			break
		}
		loopcount++

		if loopcount > flagbreaknum {
			break
		}
	}
	fmt.Println("===========result===========")

	fmt.Printf("diffcos %v | inference %v | pkposcheck %v| pkvalidity %v \n", flagdiffcos, flaginference, flagpos, flaglength)
	fmt.Println("loop count :", loopcount-1)
	fmt.Println("correct count :", correctcount)
	fmt.Println("accuracy :", float64(correctcount)/float64(loopcount-1))
	//fmt.Println("fpr :", float64(fprcount)/float64(loopcount-1))

	endtime := time.Now()
	elapsed := endtime.Sub(starttime)
	roundtm := elapsed.Milliseconds() / int64(loopcount-1)
	fmt.Println("starttime is: ", starttime.String())
	fmt.Println("end time is: ", endtime.String())
	fmt.Println("elapsed time is: ", elapsed.String(), "one round time: ", roundtm, "(ms)")
	/* debug
	for i, f := range flist {
		fmt.Println("fileid:", i, f)
	}
	*/
}
