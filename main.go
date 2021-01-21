package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"go-cron-client/app"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)

type RespCommon struct {
	Code int
	Msg  string
}

type RespClientAdd struct {
	RespCommon
}

type RespClientPing struct {
	RespCommon
}

type RespJobAdd struct {
	RespCommon
}

type RespJobRemove struct {
	RespCommon
}

type Job struct {
	Id        int          `json:"id"`
	Name      string       `json:"name"`
	Project   string       `json:"project"`
	Creator   string       `json:"creator"`
	CreatTime string       `json:"creat_time"`
	Enable    bool         `json:"enable"`
	Server    string       `json:"server"`
	Script    string       `json:"script"`
	Dir       string       `json:"dir"`
	Spec      string       `json:"spec"`
	Group     string       `json:"group"`
	Prev      string       `json:"prev"`
	Next      string       `json:"next"`
	Pid       int          `json:"pid"`
	State     string       `json:"state"`
	EntryID   cron.EntryID `json:"-"`
	Md5       string       `json:"-"`
}

type RespJobList struct {
	RespCommon
	Data []Job
}

type Client struct {
	Name   string
	Status int
	Entry  []cron.Entry
}

type Server struct {
	Name   string
	Uri    string
	Client *rpc.Client
}

var Servers = make(map[string]*Server)

var JobList = make(map[int]*Job, 0)

var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

var OK = 1
var ERR = 0
var ServerUri = "127.0.0.1:1234"
var ServerName = "server_one"
var Interval = time.Second
var ClientUri = "127.0.0.1:2234"
var CodeSuccess = 0
var CodeError = 1
var Success = "success"

var ClientInfo = &app.Client{}

var c = cron.New()

func init() {
	app.InitConfig()
	ServerUri = app.Conf.Server.Uri
	ServerName = app.Conf.Server.Name
	ClientUri = app.Conf.Client.Uri
	ClientInfo = app.Conf.Client
	Interval = time.Duration(app.Conf.Server.Interval) * time.Second
	logFile, err := os.OpenFile(app.Conf.Log.Filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("open log file failed.", err)
	}
	Info = log.New(os.Stdout, "Info:", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "Warning:", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr, logFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	c.Start()
	client := new(Client)
	if err := rpc.Register(client); err != nil {
		Error.Println("Server register failed.", err)
		return
	}
	rpc.HandleHTTP()
	listen, err := net.Listen("tcp", ClientUri)
	if err != nil {
		Error.Println("Server listen failed.", err)
		return
	}
	go func() {
		if err = http.Serve(listen, nil); err != nil {
			Error.Println("Server failed.", err)
			return
		}
	}()
	client.serverPing()
	select {}
}

func scriptExec(job Job) {
	jobId := job.Id
	script := job.Script
	dir := job.Dir
	if JobList[jobId] == nil {
		Info.Println("Job is removed.", job)
		return
	}
	pid := JobList[jobId].Pid
	if JobList[jobId].State == "RUN" {
		Info.Println("Job is in process.", job)
		return
	}
	s := strings.Split(script, " ")
	shell := exec.Command(s[0], s[1:]...)
	if dir != "" {
		shell.Dir = dir
	}
	err := shell.Start()
	if err != nil {
		Error.Println("Job run failed.", job)
		return
	}
	go func() {
		if err := shell.Wait(); err != nil {
			Info.Println("Job is killed.", job)
		} else {
			Info.Println("Job is finished.", job)
		}
		if JobList[jobId] == nil {
			Info.Println("Job is removed.", job)
			return
		}
		JobList[jobId].State = "DEF"
		JobList[jobId].Pid = 0
	}()
	JobList[jobId].State = "RUN"
	pid = shell.Process.Pid
	JobList[jobId].Pid = pid
	Info.Println(pid, shell)
}

func (client *Client) serverPing() {
	go func() {
		for {
			time.Sleep(Interval)
			if Servers[ServerName] == nil {
				respClientAdd := new(RespClientAdd)
				if err := client.ClientAdd("Client", respClientAdd); err != nil {
					//Error.Println(err.Error())
				}
				continue
			}
			respClientPing := new(RespClientPing)
			clientPing := Servers[ServerName].Client.Go("Server.ClientPing", "Client", respClientPing, nil)
			replyCall := <-clientPing.Done
			if replyCall.Error != nil || respClientPing.Code == CodeError {
				respClientAdd := new(RespClientAdd)
				if err := client.ClientAdd("Client", respClientAdd); err != nil {
					//Error.Println(err.Error())
				}
				continue
			}
			//Info.Println("Ping ok. client:", client.Name)
		}
	}()
}

func (client *Client) JobAdd(job *Job, respJobAdd *RespJobAdd) error {
	jobId := job.Id
	script := job.Script
	dir := job.Dir
	spec := job.Spec
	group := job.Group
	if group != "" && ClientInfo.Group != "" && group != ClientInfo.Group {
		Info.Println("Job add failed.", *job)
		return errors.New("cmd add failed")
	}
	if JobList[jobId] == nil {
		JobList[jobId] = &Job{}
	}
	taskMd5 := fmt.Sprintf("%x", md5.Sum([]byte(script+dir+spec+group)))
	entryID := JobList[jobId].EntryID
	if entryID > 0 {
		//Info.Println("job is in cron:", cmd)
		//修改任务
		if JobList[jobId].Md5 != taskMd5 {
			entryIDOld := entryID
			cmdOld := job
			entryID, _ = c.AddFunc(spec, func() {
				scriptExec(*job)
			})
			if entryID == 0 {
				Info.Println("Job add failed.", *job)
				return errors.New("job add failed")
			}
			JobList[jobId].Md5 = taskMd5
			JobList[jobId].EntryID = entryID
			JobList[jobId].Script = script
			JobList[jobId].Dir = dir
			JobList[jobId].Spec = spec
			JobList[jobId].Group = group
			Info.Println("Job add success.", *job)
			c.Remove(entryIDOld)
			Info.Println("Job remove success.", *cmdOld)
		}
		respJobAdd.Code = CodeSuccess
		respJobAdd.Msg = Success
		return nil
	}
	//增加任务
	entryID, _ = c.AddFunc(spec, func() {
		scriptExec(*job)
	})
	if entryID == 0 {
		delete(JobList, jobId)
		Info.Println("Job add failed.", *job)
		return errors.New("job add failed")
	}
	JobList[jobId].Md5 = taskMd5
	JobList[jobId].EntryID = entryID
	JobList[jobId].Script = script
	JobList[jobId].Dir = dir
	JobList[jobId].Spec = spec
	JobList[jobId].Group = group
	JobList[jobId].State = "DEF"
	Info.Println("Job add success.", *job)
	respJobAdd.Code = CodeSuccess
	respJobAdd.Msg = Success
	return nil
}

func (client *Client) JobRemove(job *Job, respJobRemove *RespJobRemove) error {
	jobId := job.Id
	if JobList[jobId] == nil {
		//Info.Println("job is not in cron:", cmd)
		return errors.New("job is not in cron")
	}
	entryID := JobList[jobId].EntryID
	if entryID == 0 {
		//Info.Println("Job is not in cron.", *cmd)
		return errors.New("job is not in cron")
	}
	c.Remove(entryID)
	delete(JobList, jobId)
	Info.Println("Job remove success.", *job)
	respJobRemove.Code = CodeSuccess
	respJobRemove.Msg = Success
	return nil
}

func (client *Client) JobList(args string, respJobList *RespJobList) error {
	jobList := make([]Job, 0)
	for _, ii := range JobList {
		entry := c.Entry(ii.EntryID)
		jobList = append(jobList, Job{
			Id:     ii.Id,
			Script: ii.Script,
			Dir:    ii.Dir,
			Spec:   ii.Spec,
			Group:  ii.Group,
			Prev:   entry.Prev.Format("2006-01-02 15:04:05"),
			Next:   entry.Next.Format("2006-01-02 15:04:05"),
			Pid:    ii.Pid,
			State:  ii.State,
		})
	}
	respJobList.Code = CodeSuccess
	respJobList.Msg = Success
	respJobList.Data = jobList
	return nil
}

func (client *Client) ClientAdd(args string, respClientAdd *RespClientAdd) error {
	client.Name = ClientInfo.Name
	client.Status = ERR
	conn, err := rpc.DialHTTP("tcp", ServerUri)
	if err != nil {
		Error.Println("Client Conn failed.", err)
		return errors.New("client conn failed")
	}
	clientAdd := conn.Go("Server.ClientAdd", ClientInfo, respClientAdd, nil)
	replyCall := <-clientAdd.Done
	if replyCall.Error != nil || respClientAdd.Code == CodeError {
		Error.Println("Client add failed.", err)
		return errors.New("client add failed")
	}
	client.Status = OK
	Servers[ServerName] = &Server{Client: conn}
	Info.Println("Client add success.")
	respClientAdd.Code = CodeSuccess
	respClientAdd.Msg = Success
	return nil
}

func (client *Client) ClientPing(args string, respClientPing *RespClientPing) error {
	//Info.Println("Ping ok. client:", ClientInfo.Name)
	respClientPing.Code = CodeSuccess
	respClientPing.Msg = Success
	return nil
}
