#!/usr/bin/env python3

"""
The MIT License (MIT)
Copyright © 2025 <Neuro-NX>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

"""
Usage Example:
    python vdmfd.py /path/to/search -threads=4 -filelist \
         -size 100:mb -a -size 500:mb \
         -duration 60:sec -o -filename movie

This script recursively walks a directory and for each video file calls 'ffprobe'
to get metadata. It checks various criteria concurrently using a thread pool.
If the -filelist option is provided, it outputs matching video file paths to a file.
"""

import os
import sys
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Map of unit multipliers for file size.
SIZE_UNITS = {
    "b": 1,
    "kb": 1024,
    "mb": 1024 * 1024,
    "gb": 1024 * 1024 * 1024,
}

DEFAULT_FILELIST_FILENAME = "video-filelist.txt"
DEFAULT_FILELIST_DIR = "/tmp/"

def parse_size_arg(arg):
    try:
        value, unit = arg.split(":")
        value = float(value)
        unit = unit.lower()
        if unit not in SIZE_UNITS:
            raise ValueError("Invalid size unit")
        return value, unit
    except Exception as e:
        raise ValueError(f"Invalid size argument format: {arg}") from e

def parse_duration_arg(arg):
    try:
        value, unit = arg.split(":")
        value = float(value)
        unit = unit.lower()
        factor = 1
        if unit in ["sec", "s", "seconds"]:
            factor = 1
        elif unit in ["min", "m", "minutes"]:
            factor = 60
        elif unit in ["hr", "h", "hours"]:
            factor = 3600
        else:
            raise ValueError("Invalid duration unit")
        return value * factor
    except Exception as e:
        raise ValueError(f"Invalid duration argument format: {arg}") from e

def get_video_metadata(filepath):
    cmd = [
        "ffprobe",
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        filepath
    ]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        if not output.strip():
            sys.stderr.write(f"ffprobe returned no output for file: {filepath}\n")
            return None
        meta = json.loads(output)
        if not meta or ("format" not in meta and "streams" not in meta):
            sys.stderr.write(f"ffprobe produced incomplete metadata for file: {filepath}\n")
            return None
        if not meta.get("format"):
            sys.stderr.write(f"No format information in ffprobe metadata for file: {filepath}\n")
            return None
        if not meta.get("streams"):
            sys.stderr.write(f"No streams found in ffprobe metadata for file: {filepath}\n")
            return None
        return meta
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"Error running ffprobe on file {filepath}: {e}\n")
        return None
    except json.JSONDecodeError as e:
        sys.stderr.write(f"Error parsing JSON from ffprobe output for file {filepath}: {e}\n")
        return None

def check_criteria(metadata, filepath, crit_type, crit_value):
    file_lower = os.path.basename(filepath).lower()
    if crit_type == "filename":
        return crit_value.lower() in file_lower
    elif crit_type == "container":
        ext = os.path.splitext(filepath)[1][1:].lower()
        return crit_value.lower() == ext

    fmt = metadata.get("format", {})
    duration = float(fmt.get("duration", 0))
    size = float(fmt.get("size", 0))
    bitrate = float(fmt.get("bit_rate", 0)) / 1000 if fmt.get("bit_rate") else 0

    video_stream = None
    for s in metadata.get("streams", []):
        if s.get("codec_type") == "video":
            video_stream = s
            break

    if crit_type == "duration":
        target = parse_duration_arg(crit_value)
        return duration >= target
    elif crit_type == "size":
        value, unit = parse_size_arg(crit_value)
        target_bytes = value * SIZE_UNITS[unit]
        return size >= target_bytes
    elif crit_type == "bitrate":
        try:
            value, unit = parse_size_arg(crit_value)
            target_kb = value
            if unit != "kb":
                target_kb = value * (SIZE_UNITS[unit] / SIZE_UNITS["kb"])
            return bitrate >= target_kb
        except Exception as e:
            sys.stderr.write(f"Error parsing bitrate argument {crit_value}: {e}\n")
            return False
    elif crit_type in ["width", "height"]:
        if not video_stream:
            return False
        try:
            value = float(crit_value.split(":")[0])
        except Exception:
            return False
        attr = video_stream.get(crit_type)
        if attr is None:
            return False
        return float(attr) >= value
    elif crit_type == "framerate":
        if not video_stream:
            return False
        fr_str = video_stream.get("r_frame_rate", "0/1")
        try:
            num, den = fr_str.split("/")
            fr = float(num) / float(den) if float(den) != 0 else 0
        except Exception:
            try:
                fr = float(fr_str)
            except Exception:
                fr = 0
        try:
            value = float(crit_value.split(":")[0])
        except Exception:
            return False
        return fr >= value

    return False

def parse_args(argv):
    """
    Parses command-line arguments.

    Recognized options:
        -threads=N            number of threads
        -filelist[=VALUE]     if provided, outputs results to a file.
                              VALUE (optional) can be a directory, or a full filepath.
                              By default, if VALUE is omitted, the file is /tmp/video-filelist.txt.

    The first non-option argument is the search directory.

    Returns:
        search_path: str                directory where search begins.
        criteria_list: list of tuples   each tuple (criterion, value, operator)
        threads: int                    number of threads to use.
        filelist_path: str or None      if provided, the full file path for output.
    """
    if len(argv) < 2:
        sys.stderr.write("Usage: {} <directory> [options] ...\n".format(argv[0]))
        sys.exit(1)

    search_path = None
    criteria_list = []  # Each element: (criterion, value, operator)
    threads = os.cpu_count() or 4  # Default threads.
    filelist_path = None
    pending_operator = None

    allowed_flags = {
        "-duration", "-size", "-filename", "-container", 
        "-bitrate", "-width", "-height", "-framerate"
    }
    logical_ops = {"-a": "AND", "-o": "OR"}

    args = argv[1:]
    idx = 0

    # First non-option argument is assumed as search directory.
    if not args[idx].startswith("-"):
        search_path = args[idx]
        idx += 1
    else:
        sys.stderr.write("Directory path must be the first argument.\n")
        sys.exit(1)

    while idx < len(args):
        arg = args[idx]
        if arg.startswith("-threads="):
            try:
                threads = int(arg.split("=")[1])
            except Exception as e:
                sys.stderr.write(f"Invalid thread count in {arg}: {e}\n")
                sys.exit(1)
            idx += 1
        elif arg.startswith("-filelist"):
            # -filelist may be provided as either "-filelist" or "-filelist=VALUE"
            if "=" in arg:
                filelist_value = arg.split("=", 1)[1].strip()
                # If filelist_value is empty, use default.
                if not filelist_value:
                    filelist_path = os.path.join(DEFAULT_FILELIST_DIR, DEFAULT_FILELIST_FILENAME)
                else:
                    # If it's a directory, append default filename.
                    if os.path.isdir(filelist_value) or filelist_value.endswith(os.path.sep):
                        filelist_path = os.path.join(filelist_value, DEFAULT_FILELIST_FILENAME)
                    else:
                        # Otherwise, assume it's a valid file path.
                        filelist_path = filelist_value
            else:
                filelist_path = os.path.join(DEFAULT_FILELIST_DIR, DEFAULT_FILELIST_FILENAME)
            idx += 1
        elif arg in logical_ops:
            pending_operator = logical_ops[arg]
            idx += 1
        elif arg in allowed_flags:
            crit = arg.lstrip("-")
            if idx+1 >= len(args):
                sys.stderr.write(f"Expected argument after {arg}\n")
                sys.exit(1)
            crit_value = args[idx+1]
            criteria_list.append((crit, crit_value, pending_operator))
            pending_operator = None
            idx += 2
        else:
            sys.stderr.write(f"Unknown argument: {arg}\n")
            sys.exit(1)
    return search_path, criteria_list, threads, filelist_path

def satisfies_conditions(metadata, filepath, criteria_list):
    if not criteria_list:
        return True

    crit, value, _ = criteria_list[0]
    result = check_criteria(metadata, filepath, crit, value)
    for (crit, value, operator) in criteria_list[1:]:
        current = check_criteria(metadata, filepath, crit, value)
        op_to_use = operator if operator is not None else "AND"
        if op_to_use == "AND":
            result = result and current
        elif op_to_use == "OR":
            result = result or current
    return result

def is_video(filepath):
    try:
        cmd = ["xdg-mime", "query", "filetype", filepath]
        mime_type = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode().strip()
        return mime_type.startswith("video/")
    except Exception as e:
        sys.stderr.write(f"Could not determine MIME type for {filepath}: {e}\n")
        return False

def get_and_check_file(filepath, criteria_list):
    """
    Process an individual file: check if it's a video,
    attempt to get metadata, and if conditions are met,
    return the formatted filepath for printing, or None.
    The returned filepath string does not hold the "File:" prefix.
    """
    if not is_video(filepath):
        return None
    metadata = get_video_metadata(filepath)
    if metadata is None:
        sys.stderr.write(f"Skipping file due to problematic metadata: {filepath}\n")
        return None
    if satisfies_conditions(metadata, filepath, criteria_list):
        # Remove any newline characters and enclose result in double quotes.
        result = filepath.replace("\n", "").strip()
        return f"\"{result}\""
    return None

def main():
    search_path, criteria_list, threads, filelist_path = parse_args(sys.argv)
    if not os.path.isdir(search_path):
        sys.stderr.write(f"{search_path} is not a valid directory.\n")
        sys.exit(1)

    results = []  # to hold successful file paths.
    futures = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for root, dirs, files in os.walk(search_path):
            for file in files:
                filepath = os.path.join(root, file)
                futures.append(executor.submit(get_and_check_file, filepath, criteria_list))
        for future in as_completed(futures):
            result = future.result()
            if result:
                print(f"File: {result}")  # still printing with prefix for console.
                results.append(result)

    # If filelist option was specified, write results to file.
    if filelist_path:
        try:
            # Ensure the directory exists.
            os.makedirs(os.path.dirname(filelist_path), exist_ok=True)
            with open(filelist_path, "w") as f:
                # Write each result on a new line.
                for res in results:
                    f.write(f"{res}\n")
            sys.stderr.write(f"Output to Filelist: {filelist_path}\n")
        except Exception as e:
            sys.stderr.write(f"Error writing filelist: {e}\n")

if __name__ == "__main__":
    main()
