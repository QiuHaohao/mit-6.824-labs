package raft

import (
	"errors"
	"log"
)

// Debugging
const Debug = 0

var errNoSuchEntry = errors.New("no such entry")

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(arr []int) int {
	minN := arr[0]
	for _, n := range arr {
		if n < minN {
			minN = n
		}
	}
	return minN
}
func getLogFromPartialLog(partialLog []*LogEntry, startsFromIndex int, index int) (*LogEntry, error) {
	indexInPartialLog := index - startsFromIndex
	if indexInPartialLog < 0 || !(indexInPartialLog < len(partialLog)) {
		return nil, errNoSuchEntry
	}
	return partialLog[indexInPartialLog], nil
}

func getLogSliceFromPartialLog(partialLog []*LogEntry, startsFromIndex int, startIndex int, endIndex int) []*LogEntry {
	return partialLog[startIndex-startsFromIndex : endIndex-startsFromIndex]
}
