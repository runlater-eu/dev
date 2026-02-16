package main

import (
	"fmt"
	"os"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorRed    = "\033[31m"
	colorDim    = "\033[2m"
)

var noColor bool

func timestamp() string {
	return time.Now().Format("15:04:05")
}

func c(color, text string) string {
	if noColor {
		return text
	}
	return color + text + colorReset
}

func logInbound(method, path, detail string) {
	fmt.Fprintf(os.Stderr, "%s %s %-6s %-40s %s\n",
		c(colorDim, timestamp()),
		c(colorCyan, "-->"),
		method, path,
		c(colorDim, detail))
}

func logOutbound(method, url string) {
	fmt.Fprintf(os.Stderr, "%s %s %-6s %s\n",
		c(colorDim, timestamp()),
		c(colorYellow, " ->"),
		method, url)
}

func logResult(statusCode int, durationMs int) {
	color := colorGreen
	if statusCode >= 400 {
		color = colorRed
	}
	fmt.Fprintf(os.Stderr, "%s %s %s %s\n",
		c(colorDim, timestamp()),
		c(color, " <-"),
		c(color, fmt.Sprintf("%d", statusCode)),
		c(colorDim, fmt.Sprintf("[%dms]", durationMs)))
}

func logError(msg string) {
	fmt.Fprintf(os.Stderr, "%s %s %s\n",
		c(colorDim, timestamp()),
		c(colorRed, " !!"),
		c(colorRed, msg))
}
