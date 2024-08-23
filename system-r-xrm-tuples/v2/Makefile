.PHONY: clean create
N=1000

bench: create
	uv run python -m cProfile -o prof xrd.py bench --N $(N)
	uv run gprof2dot --colour-nodes-by-selftime -f pstats prof | dot -Tpng -o output.png
	#uv run kernprof -l -v xrd.py bench --N $(N)

create:
	uv run python xrd.py create --N $(N)

clean:
	rm -rf fields tuples relations

