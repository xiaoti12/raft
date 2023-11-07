package raft

import "log"

// Debugging
const (
	Debug = 1
	Reset = "\033[0m"
	RED   = "\033[31m"
	Green = "\033[32m"
	BLUE  = "\033[34m"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
func TestPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		print(Green)
		log.Printf("TEST: "+format, a...)
		print(Reset)
	}
	return
}
func LeaderPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		print(BLUE)
		log.Printf(format, a...)
		print(Reset)
	}
	return
}
func BadPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		print(RED)
		log.Printf(format, a...)
		print(Reset)
	}
	return
}
