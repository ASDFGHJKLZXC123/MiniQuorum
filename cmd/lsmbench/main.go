package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"miniquorum/bench"
)

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit("missing subcommand")
	}

	sub := os.Args[1]
	switch sub {
	case "collect":
		runCollect(os.Args[2:])
	case "validate":
		runValidate(os.Args[2:])
	case "graph":
		runGraph(os.Args[2:])
	default:
		printUsageAndExit("unknown subcommand: " + sub)
	}
}

func runCollect(args []string) {
	flags := flag.NewFlagSet("collect", flag.ContinueOnError)
	var out string
	var profile string
	flags.SetOutput(new(bytes.Buffer))
	flags.StringVar(&out, "out", "", "raw output path")
	flags.StringVar(&profile, "profile", "", "benchmark profile: smoke-v1 or report-v1")
	if err := flags.Parse(args); err != nil {
		exitWithUsage("collect: "+err.Error(), true)
	}
	if flags.NArg() != 0 {
		exitWithUsage(fmt.Sprintf("collect: unexpected trailing args: %v", flags.Args()), true)
	}
	if profile == "" {
		exitWithUsage("collect: -profile is required", true)
	}
	if out == "" {
		exitWithUsage("collect: -out is required", true)
	}
	if profile != bench.ProfileSmoke && profile != bench.ProfileReport {
		exitWithUsage(fmt.Sprintf("collect: unknown profile %q", profile), true)
	}
	if _, err := bench.Collect(profile, out); err != nil {
		exitf("collect: %v\n", err)
	}
}

func runValidate(args []string) {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	var in string
	flags.SetOutput(new(bytes.Buffer))
	flags.StringVar(&in, "in", "", "raw file to validate")
	if err := flags.Parse(args); err != nil {
		exitWithUsage("validate: "+err.Error(), true)
	}
	if flags.NArg() != 0 {
		exitWithUsage(fmt.Sprintf("validate: unexpected trailing args: %v", flags.Args()), true)
	}
	if in == "" {
		exitWithUsage("validate: -in is required", true)
	}
	if _, err := bench.ReadRawResult(in); err != nil {
		exitf("validate: %v\n", err)
	}
}

func runGraph(args []string) {
	flags := flag.NewFlagSet("graph", flag.ContinueOnError)
	var in string
	var outDir string
	flags.SetOutput(new(bytes.Buffer))
	flags.StringVar(&in, "in", "", "raw file to graph")
	flags.StringVar(&outDir, "out-dir", "", "output directory for SVG files")
	if err := flags.Parse(args); err != nil {
		exitWithUsage("graph: "+err.Error(), true)
	}
	if flags.NArg() != 0 {
		exitWithUsage(fmt.Sprintf("graph: unexpected trailing args: %v", flags.Args()), true)
	}
	if in == "" {
		exitWithUsage("graph: -in is required", true)
	}
	if outDir == "" {
		exitWithUsage("graph: -out-dir is required", true)
	}
	if err := bench.GenerateGraphsFromFile(in, outDir); err != nil {
		exitf("graph: %v\n", err)
	}
}

func printUsageAndExit(detail string) {
	fmt.Fprintln(os.Stderr, detail)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  lsmbench collect -profile smoke-v1|report-v1 -out <raw.json>")
	fmt.Fprintln(os.Stderr, "  lsmbench validate -in <raw.json>")
	fmt.Fprintln(os.Stderr, "  lsmbench graph -in <raw.json> -out-dir <directory>")
	os.Exit(2)
}

func exitWithUsage(message string, exit bool) {
	fmt.Fprintln(os.Stderr, message)
	if exit {
		printUsageAndExit(message)
	}
}

func exitf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	os.Exit(2)
}
