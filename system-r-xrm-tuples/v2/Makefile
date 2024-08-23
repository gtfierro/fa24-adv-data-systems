.PHONY: clean bench-xrd bench-phase1
N=1000000

bench-xrd: xrd
	time uv run python -m cProfile -o prof xrd.py bench --N $(N)
	uv run gprof2dot --colour-nodes-by-selftime -f pstats prof | dot -Tpng -o output.png

xrd:
	uv run python xrd.py create --N $(N)

bench-phase1: phase1
	time uv run python -m cProfile -o prof phase1.py bench --N $(N)
	uv run gprof2dot --colour-nodes-by-selftime -f pstats prof | dot -Tpng -o output.png

phase1:
	uv run python phase1.py create --N $(N)

clean:
	rm -rf xrd phase1

