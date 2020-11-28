package main

import (
    "os"
    "io"
    "fmt"
    "log"
    "flag"
    "runtime"
    "github.com/cheggaaa/pb"
    "github.com/dustin/go-humanize"
	"github.com/kkdd/osmpbf"
	"encoding/csv"
	"strconv"
)

func main() {
    ncpu := flag.Int("ncpu", 1, "number of CPU")
    flag.Parse()
    runtime.GOMAXPROCS(*ncpu)
    for _, file := range flag.Args() {
        worker(file)
    }
}

func worker(file string) { 
    f, err := os.Open(file)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
    stat, _ := f.Stat()
    filesiz := int(stat.Size()/1024)

    d := osmpbf.NewDecoder(f)
    err = d.Start(runtime.GOMAXPROCS(-1))
    if err != nil {
        log.Fatal(err)
	}
	
	// 出力用ファイル
    node_file, node_err := os.Create("./node.csv")
    if node_err != nil {
        panic(node_err)
    }
	defer node_file.Close()
	node_writer := csv.NewWriter(node_file) // utf8
	node_writer.Write([]string{"id","lat","lon"})

	// 出力用ファイル
    way_file, way_err := os.Create("./edge.csv")
    if way_err != nil {
        panic(way_err)
    }
	defer way_file.Close()
	way_writer := csv.NewWriter(way_file) // utf8
	way_writer.Write([]string{"id","node1","node2"})
	
	// // 出力用ファイル
    // way_file, way_err := os.Open("./edge.csv")
    // if way_err != nil {
    //     panic(way_err)
    // }
	// defer way_file.Close()
	
    var nc, wc, rc, i int64
    progressbar := pb.New(filesiz).SetUnits(pb.U_NO)
    progressbar.Start()
    for i = 0; ; i++ {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {
            switch v := v.(type) {
			case *osmpbf.Node:
				node_writer.Write([]string{strconv.FormatInt(v.ID,10),strconv.FormatFloat(v.Lat, 'f', -1, 64),strconv.FormatFloat(v.Lon, 'f', -1, 64)})
                nc++
            case *osmpbf.Way:
				for i:=1;i<len(v.NodeIDs);i++{
					way_writer.Write([]string{strconv.FormatInt(v.ID,10),strconv.FormatInt(v.NodeIDs[i-1],10),strconv.FormatInt(v.NodeIDs[i],10)})
				}
                wc++
            case *osmpbf.Relation:
                rc++
            default:
                log.Fatalf("unknown type %T\n", v)
            }
        }
        if i % 131072 == 0 {
            progressbar.Set(int(d.GetTotalReadSize()/1024))
        }
    }
    progressbar.Set(filesiz)
    progressbar.Finish()
	fmt.Printf("Nodes: %s, Ways: %s, Relations: %s\n", humanize.Comma(nc), humanize.Comma(wc), humanize.Comma(rc))
	
	fmt.Println(nc)

	node_writer.Flush()
	way_writer.Flush()
}