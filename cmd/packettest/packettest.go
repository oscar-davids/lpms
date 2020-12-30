package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/livepeer/lpms/ffmpeg"
)

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
	idprofile := 5
	idmode := 6
	idindices := 8
	for i, value := range field {
		if value == "filepath" {
			idpath = i
		}
		if value == "profile" {
			idprofile = i
		}
		if value == "devmode" {
			idmode = i
		}
		if value == "indices" {
			idindices = i
		}
	}
	return idpath, idprofile, idmode, idindices
}

func main() {
	if len(os.Args) != 4 {
		panic("Usage: <input csv file> <directory source video path> <out csv file>")
	}
	fcsvname := os.Args[1]
	indir := os.Args[2]
	fwcsvname := os.Args[3]

	wd, _ := os.Getwd()
	workDir := wd + "/tmp"
	os.RemoveAll(workDir)
	os.Mkdir(workDir, 0700)
	fwcsvname = workDir + "/" + fwcsvname

	str2accel := func(inp string) (ffmpeg.Acceleration, string) {
		if inp == "nv" {
			return ffmpeg.Nvidia, "nv"
		}
		return ffmpeg.Software, "sw"
	}
	str2profs := func(inp string) []ffmpeg.VideoProfile {
		profs := []ffmpeg.VideoProfile{}
		strs := strings.Split(inp, ",")
		for _, k := range strs {
			p, ok := ffmpeg.VideoProfileLookup[k]
			if !ok {
				panic(fmt.Sprintf("Invalid rendition %s. Valid renditions are:\n%s", k, validRenditions()))
			}
			profs = append(profs, p)
		}
		return profs
	}

	f, err := os.Open(fcsvname)
	defer f.Close()

	if err != nil {
		panic("no input csv file!")
	}

	r := csv.NewReader(f)
	//read first field
	field, err := r.Read()
	idpath, idprofile, idmode, idindices := getids(field)

	//
	fw, err := os.Create(fwcsvname)
	defer fw.Close()
	if err != nil {
		panic("use: corect output csv file")
	}
	w := csv.NewWriter(fw)
	defer w.Flush()
	//write header
	//wrecord := []string{"filepath", "position", "length"}
	wrecord := field
	wrecord = append(wrecord, "outpath")
	wrecord = append(wrecord, "position")
	wrecord = append(wrecord, "length")

	err = w.Write(wrecord)
	lognum := 0
	starttime := time.Now()
	//fmt.Printf("\n ========== %v %v %v %v \n", idpath, idprofile, idmode, idindices)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		//fmt.Printf("%v\n", record)
		fname := indir + "/" + record[idpath]
		profiles := str2profs(record[idprofile])
		accel, lbl := str2accel(record[idmode])
		indicies := record[idindices]
		indicies = strings.ReplaceAll(indicies, "\"", "")

		infilename := filepath.Base(fname)
		infilename = FilenameWithoutExtension(infilename)
		outfilename := fmt.Sprintf("%s/%d_%s_%s_out.ts", workDir, lognum, infilename, lbl)

		profs2opts := func(profs []ffmpeg.VideoProfile) []ffmpeg.TranscodeOptions {
			opts := []ffmpeg.TranscodeOptions{}
			for i := range profs {
				o := ffmpeg.TranscodeOptions{
					//Oname:   fmt.Sprintf("%s/%s_%s_%d_out.ts", workDir, infilename, lbl, i),
					Oname:   outfilename,
					Profile: profs[i],
					Accel:   accel,
				}
				opts = append(opts, o)
			}
			return opts
		}

		options := profs2opts(profiles)
		var dev string
		if accel == ffmpeg.Nvidia {
			dev = "0"
		}

		fmt.Printf("\n %v ========== start transcoding %v %v \n", lognum, record[idpath], indicies)

		ffmpeg.InitFFmpeg()
		//fmt.Printf("Setting fname %s encoding %d renditions with %v\n", fname, len(options), lbl)
		res, err := ffmpeg.Transcode3(&ffmpeg.TranscodeOptionsIn{
			Fname:   fname,
			Accel:   accel,
			Device:  dev,
			Indices: indicies,
		}, options)
		if err != nil {
			panic(err)
		}
		fmt.Printf("profile=input frames=%v pixels=%v\n", res.Decoded.Frames, res.Decoded.Pixels)
		for i, r := range res.Encoded {
			fmt.Printf("profile=%v frames=%v pixels=%v pos=%v len=%v\n",
				profiles[i].Name, r.Frames, r.Pixels, r.Positions, r.Lengths)
			if i == 0 {
				wrecord := record
				wrecord = append(wrecord, "\""+outfilename+"\"")
				wrecord = append(wrecord, "\""+r.Positions+"\"")
				wrecord = append(wrecord, "\""+r.Lengths+"\"")
				//wrecord[0] = record[idpath]
				//wrecord[1] = "\"" + r.Positions + "\""
				//wrecord[2] = "\"" + r.Lengths + "\""
				err = w.Write(wrecord)
			}
		}

		lognum++
	}
	endtime := time.Now()
	elapsed := endtime.Sub(starttime)
	fmt.Println("starttime is: ", starttime.String())
	fmt.Println("end time is: ", endtime.String())
	fmt.Println("elapsed time is: ", elapsed.String())
}
