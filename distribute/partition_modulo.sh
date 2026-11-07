#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PARTITIONER="$ROOT/build/bin/build_partitions_from_v2p"

usage() {
    cat <<'EOF'
Usage:
  partition_modulo.sh -v VERTICES -e EDGES -o OUTPUT -n PARTITIONS

Input formats:
  VERTICES: one unsigned vertex ID per line
  EDGES:    "source label destination" per line

The owner of vertex v is v % PARTITIONS. Each edge is stored in the owner
partition of its source vertex. OUTPUT must not already exist.
EOF
}

vertex_file=""
edge_file=""
output_dir=""
partitions=""

while (($#)); do
    case "$1" in
        -v|--vertex-file)
            (($# >= 2)) || { usage >&2; exit 2; }
            vertex_file="$2"
            shift 2
            ;;
        -e|--edge-file)
            (($# >= 2)) || { usage >&2; exit 2; }
            edge_file="$2"
            shift 2
            ;;
        -o|--output-dir)
            (($# >= 2)) || { usage >&2; exit 2; }
            output_dir="$2"
            shift 2
            ;;
        -n|--num-partitions)
            (($# >= 2)) || { usage >&2; exit 2; }
            partitions="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ -n "$vertex_file" && -n "$edge_file" && -n "$output_dir" && -n "$partitions" ]] || {
    usage >&2
    exit 2
}
[[ -f "$vertex_file" ]] || { printf 'Missing vertex file: %s\n' "$vertex_file" >&2; exit 1; }
[[ -f "$edge_file" ]] || { printf 'Missing edge file: %s\n' "$edge_file" >&2; exit 1; }
[[ "$partitions" =~ ^[1-9][0-9]*$ ]] || {
    printf 'PARTITIONS must be a positive integer: %s\n' "$partitions" >&2
    exit 2
}
[[ ! -e "$output_dir" ]] || {
    printf 'Refusing to overwrite existing output: %s\n' "$output_dir" >&2
    exit 1
}

if [[ ! -x "$PARTITIONER" ]]; then
    "$ROOT/build.sh"
fi

output_parent="$(dirname -- "$output_dir")"
output_name="$(basename -- "$output_dir")"
mkdir -p "$output_parent"
work_dir="$(mktemp -d "$output_parent/.${output_name}.partitioning.XXXXXX")"
v2p_file="$work_dir/v2p.input.txt"
staged_output="$work_dir/output"

cleanup() {
    rm -rf -- "$work_dir"
}
trap cleanup EXIT INT TERM

awk -v partitions="$partitions" '
    NF == 0 { next }
    NF != 1 || $1 !~ /^[0-9]+$/ {
        printf "Invalid vertex line %d: %s\n", NR, $0 > "/dev/stderr"
        exit 1
    }
    { print $1, $1 % partitions }
' "$vertex_file" > "$v2p_file"

"$PARTITIONER" \
    -v "$vertex_file" \
    -e "$edge_file" \
    -m "$v2p_file" \
    -o "$staged_output" \
    -n "$partitions"

# Keep the router metadata valid after moving the staged directory.
printf 'strategy=v2p\nv2p_file=%s/v2p.txt\n' "$output_dir" \
    > "$staged_output/meta.txt"

mv -- "$staged_output" "$output_dir"
printf 'partition_root=%s\n' "$output_dir"
