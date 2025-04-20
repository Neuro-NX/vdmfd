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

class Colors:
    # FIXME: How to both color text and make it bold?
    def __init__(self):
        self.drk_yellow = '\033[33m'
        self.red = '\033[91m'
        self.green = '\033[92m'
        self.blue = '\033[94m'
        self.yellow = '\033[93m'
        self.magenta = '\033[95m'
        self.cyan = '\033[96m'
        self.white = '\033[97m'
        self.bold = '\033[1m'
        self.underline = '\033[4m'
        self.reset = '\033[0m'
    def colorize(self, text, color):
        color_code = getattr(self, color.lower(), '')
        return f"{color_code}{text}{self.reset}"

colors = Colors()

# Map of unit multipliers for file size.
SIZE_UNITS = {
    "b": 1,
    "kb": 1024,
    "mb": 1024 * 1024,
    "gb": 1024 * 1024 * 1024,
}

DEFAULT_FILELIST_FILENAME = "vdmfd_filelist.txt"
DEFAULT_FILELIST_DIR = os.getcwd()

VIDEO_EXT = {
    '.avi',
    '.divx',
    '.flv',
    '.mkv',
    '.mov',
    '.mp4',
    '.m4v',
    '.mpeg',
    '.mpg',
    '.webm',
    '.wmv',
    '.3gp',
}

VIDEO_PARTS_EXT = {
    # Extensions that could contain packets of partial video data.
    # These are currently excluded from the search.
    '.ts',
    '.seg',
    '.part',
    '.crdownload',
    '.exi',
    '.exo',
    '.ogv',
    '.ogm',
    '.ogg',
    '.m4s'
}

# Allowed comparison operators (these are for the value comparison)
ALLOWED_COMPARISONS = {"-gt", "-gte", "-lt", "-lte"}

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

def check_criteria(metadata, filepath, crit_type, comp_op, crit_value):
    if crit_type == "path":
        path_lower = os.path.dirname(filepath).lower()
        return crit_value.lower() in path_lower
    elif crit_type == "filename":
        file_lower = os.path.basename(filepath).lower()
        return crit_value.lower() in file_lower
    elif crit_type == "container":
        ext = os.path.splitext(filepath)[1][1:].lower()
        return crit_value.lower() == ext

    fmt = metadata.get("format", {})
    duration = float(fmt.get("duration", 0))
    size = float(fmt.get("size", 0))
    bitrate = float(fmt.get("bit_rate", 0)) if fmt.get("bit_rate") else 0

    video_stream = None
    for s in metadata.get("streams", []):
        if s.get("codec_type") == "video":
            video_stream = s
            break

    if crit_type == "duration":
        target = parse_duration_arg(crit_value)
        if comp_op == "eq":
            return math.isclose(duration, target, rel_tol=0.01)
        elif comp_op == "gt":
            return duration > target
        elif comp_op == "gte":
            return duration >= target
        elif comp_op == "lt":
            return duration < target
        elif comp_op == "lte":
            return duration <= target
    elif crit_type == "size":
        value, unit = parse_size_arg(crit_value)
        target_bytes = value * SIZE_UNITS[unit]
        if comp_op == "eq":
            return math.isclose(size, target_bytes, rel_tol=0.01)
        elif comp_op == "gt":
            return size > target_bytes
        elif comp_op == "gte":
            return size >= target_bytes
        elif comp_op == "lt":
            return size < target_bytes
        elif comp_op == "lte":
            return size <= target_bytes
    elif crit_type == "bitrate":
        try:
            value, unit = parse_size_arg(crit_value)
            target_kbps = value
            # ffprobe will return value in unit 'kbps'.
            if unit == "kb":
                target_kbps = value
            elif unit == "mb":
                target_kbps = value * 1000
            elif unit == "gb":
                target_kbps = value * 1000000
            if comp_op == "eq":
                return math.isclose(bitrate, target_kbps, rel_tol=0.01)
            elif comp_op == "gt":
                return bitrate > target_kbps
            elif comp_op == "gte":
                return bitrate >= target_kbps
            elif comp_op == "lt":
                return bitrate < target_kbps
            elif comp_op == "lte":
                return bitrate <= target_kbps
        except Exception as e:
            sys.stderr.write(f"Error parsing bitrate argument {crit_value}: {e}\n")
            return False
    elif crit_type == "codec_name":
        if not video_stream:
            return False
        codec_name = video_stream.get(crit_type)
        if codec_name is None:
            return False
        return crit_value.lower() == codec_name.lower()
    elif crit_type == "codec_tag":
        if not video_stream:
            return False
        codec_tag = video_stream.get("codec_tag_string")
        if codec_tag is None:
            return False
        return crit_value.lower() == codec_tag.lower()
    elif crit_type == "aspect":
        if not video_stream:
            return False
        aspect = video_stream.get("display_aspect_ratio")
        if aspect is None:
            return False
        return crit_value == aspect
    elif crit_type in ["width", "height"]:
        if not video_stream:
            return False
        try:
            target = float(crit_value.split(":")[0])
        except Exception:
            return False
        attr = video_stream.get(crit_type)
        if attr is None:
            return False
        current = float(attr)
        if comp_op == "eq":
            return math.isclose(current, target, rel_tol=0.01)
        elif comp_op == "gt":
            return current > target
        elif comp_op == "gte":
            return current >= target
        elif comp_op == "lt":
            return current < target
        elif comp_op == "lte":
            return current <= target
    elif crit_type == "orientation":
        if not video_stream:
            return False
        width = video_stream.get("width")
        height = video_stream.get("height")
        if width is None or height is None:
            return False
        w = float(width)
        h = float(height)
        orientation = crit_value.lower()
        if orientation == "landscape" or orientation == "l":
            return w > h
        elif orientation == "portrait" or orientation == "p":
            return w < h
        elif orientation == "square" or orientation == "sq":
            return w == h
    elif crit_type == "pix_fmt":
        if not video_stream:
            return False
        pix_fmt = video_stream.get(crit_type)
        if pix_fmt is None:
            return False
        return crit_value.lower() == pix_fmt.lower()
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
            target = float(crit_value.split(":")[0])
        except Exception:
            return False
        if comp_op == "eq":
            return math.isclose(fr, target, rel_tol=0.01)
        elif comp_op == "gt":
            return fr > target
        elif comp_op == "gte":
            return fr >= target
        elif comp_op == "lt":
            return fr < target
        elif comp_op == "lte":
            return fr <= target
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
        "-aspect",
        "-bitrate",
        "-codec_name",
        "-codec_tag",
        "-container",
        "-duration",
        "-filename",
        "-framerate",
        "-height",
        "-orientation",
        "-path",
        "-pix_fmt",
        "-size",
        "-width",
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
            comp_operator = "eq"  # default comparison
            # Check if the next token is a comparison operator.
            if idx + 1 < len(args) and args[idx + 1] in ALLOWED_COMPARISONS:
                comp_operator = args[idx+1].lstrip("-")
                idx += 1  # Consume the comparison operator.
            if idx+1 >= len(args):
                sys.stderr.write(f"Expected argument after {arg}\n")
                sys.exit(1)
            crit_value = args[idx+1]
            criteria_list.append((crit, comp_operator, crit_value, pending_logical))
            pending_logical = None
            idx += 2
        else:
            sys.stderr.write(f"Unknown argument: {arg}\n")
            sys.exit(1)
    return search_path, criteria_list, threads, filelist_path

def satisfies_conditions(metadata, filepath, criteria_list):
    if not criteria_list:
        return True

    # Start with the first criterion.
    crit, comp_operator, value, logical_operator = criteria_list[0]
    result = check_criteria(metadata, filepath, crit, comp_operator, value)
    for (crit, comp_operator, value, logical_operator) in criteria_list[1:]:
        current = check_criteria(metadata, filepath, crit, comp_operator, value)
        # If no logical_operator is provided, default to AND.
        op_to_use = logical_operator if logical_operator is not None else "AND"
        if op_to_use == "AND":
            result = result and current
        elif op_to_use == "OR":
            result = result or current
    return result

def is_video(filepath):
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    if ext in VIDEO_EXT:
        return True
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
            with open(filelist_path, "a") as f:
                # Append each result on a new line.
                for res in results:
                    f.write(f"{res}\n")
            sys.stderr.write(f"Output to file: \n{filelist_path}\n")
        except Exception as e:
            sys.stderr.write(f"Error writing filelist: {e}\n")

if __name__ == "__main__":
    main()
