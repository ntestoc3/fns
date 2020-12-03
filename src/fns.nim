import argparse
import streams
import nre
import strutils
import threadpool, locks
import cpuinfo
import sequtils
import terminal
import strformat
from os import sleep
import system

{.experimental: "parallel".}

const MAGIC = "FNS"

proc get_col_count(line : string) : int8 =
  line.split(re"[\s-+=]+").len.int8()

var write_lock : Lock
var total_count = 0

proc show_total_status() =
  var prev = 0
  while true:
    sleep(1000)
    stdout.eraseLine()
    let
      curr = total_count
      num_per_s = total_count - prev
    stdout.write(fmt"speed {num_per_s}/s")
    prev = curr
    stdout.flushFile()

proc proc_line(f : Stream, wf : Stream, col_count : int) =
  var line = ""
  while true:
    try:
      if f.readLine(line):
        let r = line.split(re"[\s-+=]+").map(
          proc(x: string) : int = x.strip().parseint())
        if len(r) < col_count:
          echo line, " column is ", len(r), " less than col ", col_count
        elif len(r) > col_count:
          #如果列比较多，忽略前面的列
          withLock write_lock:
            let start = len(r) - col_count
            for v in r[start .. ^1]:
              wf.write(uint64(v))
        else:
          withLock write_lock:
            for v in r:
              wf.write(uint64(v))
        total_count += 1
      else:
        return
    except:
      echo "errorr process line:", line, " message: ", getCurrentExceptionMsg(), " stack trace: ", getStackTrace()

proc trans_file_format(fromFile : string, toFile : string, skipHeaderLine : bool) : void =
  var
    f = newFileStream(fromFile, fmRead)
    wf = newFileStream(toFile, fmWrite)
    col_count = 0
    line = ""

  if skipHeaderLine:
    discard f.readLine(line)
    echo "skip header line:", line

  if f.peekLine(line):
    col_count = get_col_count(line)
    echo "source file contains ", col_count, " column."
    wf.write(MAGIC)
    wf.write(uint8(col_count))

  initLock(write_lock)
  let n = countProcessors() - 1
  for i in 0 ..< max(1, n):
    spawn proc_line(f, wf, col_count)

  # 无法达成退出条件
  # spawn show_total_status()
  sync()
  f.close()
  wf.close()
  echo "proc over, total:", total_count

proc search_number(file : string, column : int, n : int) : void =
  var f = newFileStream(file, fmRead)
  if f.readStr(len(MAGIC)) != MAGIC:
    echo file, " not a valid fns data file."
    quit(-1)
  let
    col_count = f.readInt8()
    start_pos = f.getPosition()
    value_byte_len = 8
    row_byte_len = col_count * value_byte_len
  echo "total column ", col_count
  echo "search for:", n
  var row = 0
  try:
    while true:
      f.setPosition(start_pos + row_byte_len * row + value_byte_len * column)
      let v = f.readUint64()
      if v == uint64(n):
        f.setPosition(start_pos + row_byte_len * row)
        echo "found at row:", row
        for i in 0 ..< col_count:
          let vv = f.readUint64()
          stdout.write vv
          stdout.write "  "
        echo ""
        break
      row += 1
  except IOError:
    echo "search over."

  echo "search ok."
  f.close()

when isMainModule:
  var
    p = newParser("fns"):
      help("fast number search")
      option("-f", "--from-file", help="which file used to convert binary format datafile.")
      flag("-s", "--skip-header-line", help="skip file header line.")
      flag("-b", "--build", help="build binary format file")
      option("-c", "--column", help="which column to find.", default="0")
      arg("datafile")
      arg("numbers", nargs = -1)
  try:
    let opts = p.parse(commandLineParams())
    if opts.build :
      trans_file_format(opts.from_file, opts.datafile, opts.skip_header_line)
    elif opts.help:
      quit()
    else:
      search_number(opts.datafile, parseInt(opts.column), parseInt(opts.numbers[0]))
  except:
    echo p.help
    quit(1)

