package raft

import "log"

// Debugging
const (
	Debug  = 1
	Reset  = "\033[0m"
	RED    = "\033[31m"
	Green  = "\033[32m"
	YELLOW = "\033[33m"
	BLUE   = "\033[34m"
	Cyan   = "\033[36m"
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
func ElectPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		print(YELLOW)
		log.Printf(format, a...)
		print(Reset)
	}
	return
}
