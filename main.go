package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type Task struct {
	EntryID cron.EntryID
	Cmd     *Cmd
	Pid     int
	Md5     string
}

type Cmd struct {
	Id     int
	Script string
	Dir    string
	Spec   string
}

type RespAdd struct {
	Code int
	Msg  string
}

type RespPing struct {
	Code int
	Msg  string
}

type RespAddCmd struct {
	Code int
	Msg  string
}

type RespAddRemove struct {
	Code int
	Msg  string
}

type Job struct {
	Script string
}

type RespListCmd struct {
	Code int
	Msg  string
	Data []Job
}

var TaskMap = make(map[int]*Task, 0)

var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

var OK = 1
var ERR = 0
var ServerUri = "127.0.0.1:1234"
var ClientUri = "127.0.0.1:2234"
var CodeSuccess = 0
var Success = "success"

var c = cron.New()

func init() {
	logFile, err := os.OpenFile("cron.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("open log file failed")
	}
	Info = log.New(os.Stdout, "Info:", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "Warning:", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr, logFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	c.Start()
	client := new(Client)
	if err := rpc.Register(client); err != nil {
		Error.Println("Server register failed")
		return
	}
	rpc.HandleHTTP()
	listen, err := net.Listen("tcp", ClientUri)
	if err != nil {
		Error.Println("Server listen failed")
		return
	}
	go func() {
		if err = http.Serve(listen, nil); err != nil {
			Error.Println("Server failed")
			return
		}
	}()
	respAdd := new(RespAdd)
	client.Add("", respAdd)
	select {}
}

func execScript(cmd Cmd) {
	taskId := cmd.Id
	script := cmd.Script
	dir := cmd.Dir
	pid := TaskMap[taskId].Pid
	if pid > 0 {
		s, err := infoScript(pid)
		if err == nil && len(s) > 0 {
			switch s[0:1] {
			case "R",
				"S":
				//Info.Println("cmd is in process:", cmd)
				return
			}
		}
	}
	s := strings.Split(script, " ")
	shell := exec.Command(s[0], s[1:]...)
	if dir != "" {
		shell.Dir = dir
	}
	err := shell.Start()
	if err != nil {
		Error.Println("cmd run failed:", cmd)
		return
	}
	pid = shell.Process.Pid
	TaskMap[taskId].Pid = pid
	Info.Println(pid, shell)
}

func infoScript(pid int) (string, error) {
	shell := exec.Command("ps", "h", "-o", "stat", "-p", strconv.Itoa(pid))
	out, err := shell.Output()
	if err != nil {
		return "", err
	}
	s := string(out)
	s = strings.Replace(s, "STAT", "", -1)
	s = strings.Replace(s, "\n", "", -1)
	s = strings.Replace(s, " ", "", -1)
	return s, err
}

type Client struct {
	Status int
	Entry  []cron.Entry
}

func (client *Client) AddCmd(cmd *Cmd, respAddCmd *RespAddCmd) error {
	taskId := cmd.Id
	script := cmd.Script
	dir := cmd.Dir
	spec := cmd.Spec
	if TaskMap[taskId] == nil {
		TaskMap[taskId] = &Task{}
	}
	taskMd5 := fmt.Sprintf("%x", md5.Sum([]byte(script+dir+spec)))
	entryID := TaskMap[taskId].EntryID
	if entryID > 0 {
		//Info.Println("cmd is in cron:", cmd)
		//修改任务
		if TaskMap[taskId].Md5 != taskMd5 {
			entryIDOld := entryID
			cmdOld := cmd
			entryID, _ = c.AddFunc(spec, func() {
				execScript(*cmd)
			})
			if entryID == 0 {
				Info.Println("add cmd failed:", cmd)
				return errors.New(fmt.Sprintf("add cmd failed: %v", cmd))
			}
			TaskMap[taskId].Md5 = taskMd5
			TaskMap[taskId].EntryID = entryID
			TaskMap[taskId].Cmd = cmd
			Info.Println("add cmd from cron:", cmd)
			c.Remove(entryIDOld)
			Info.Println("remove cmd from cron:", cmdOld)
		}
		respAddCmd.Code = CodeSuccess
		respAddCmd.Msg = Success
		return nil
	}
	//增加任务
	entryID, _ = c.AddFunc(spec, func() {
		execScript(*cmd)
	})
	if entryID == 0 {
		delete(TaskMap, taskId)
		Info.Println("add cmd failed:", cmd)
		return errors.New(fmt.Sprintf("add cmd failed: %v", cmd))
	}
	TaskMap[taskId].Md5 = taskMd5
	TaskMap[taskId].EntryID = entryID
	TaskMap[taskId].Cmd = cmd
	Info.Println("add cmd to cron:", cmd)
	respAddCmd.Code = CodeSuccess
	respAddCmd.Msg = Success
	return nil
}

func (client *Client) RemoveCmd(cmd *Cmd, respAddRemove *RespAddRemove) error {
	taskId := cmd.Id
	if TaskMap[taskId] == nil {
		//Info.Println("cmd is not in cron:", cmd)
		return errors.New(fmt.Sprintf("cmd is not in cron: %v", cmd))
	}
	entryID := TaskMap[taskId].EntryID
	if entryID == 0 {
		//Info.Println("cmd is not in cron:", cmd)
		return errors.New(fmt.Sprintf("cmd is not in cron: %v", cmd))
	}
	c.Remove(entryID)
	delete(TaskMap, taskId)
	Info.Println("remove cmd from cron:", cmd)
	respAddRemove.Code = CodeSuccess
	respAddRemove.Msg = Success
	return nil
}
func (client *Client) ListCmd(cmd *Cmd, respListCmd *RespListCmd) error {
	listJob := make([]Job, 0)
	for _, ii := range TaskMap {
		listJob = append(listJob, Job{
			Script: ii.Cmd.Script,
		})
	}
	respListCmd.Code = CodeSuccess
	respListCmd.Msg = Success
	respListCmd.Data = listJob
	return nil
}

func (client *Client) Add(args string, respAdd *RespAdd) error {
	client.Status = ERR
	conn, err := rpc.DialHTTP("tcp", ServerUri)
	if err != nil {
		Error.Println("conn failed:", ClientUri)
		return errors.New("conn failed")
	}
	add := conn.Go("Server.Add", ClientUri, respAdd, nil)
	replyCall := <-add.Done
	if replyCall.Error != nil {
		Error.Println("add failed:", ClientUri)
		return errors.New("add failed")
	}
	if respAdd.Code == CodeSuccess {
		client.Status = OK
	}
	Info.Println("add success")
	return nil
}

func (client *Client) Ping(args string, respPing *RespPing) error {
	respPing.Code = OK
	respPing.Msg = Success
	return nil
}
