#cat prompts.md | llm | tee 0.py

# method-based profiling
uv run python -m cProfile -o prof 0.py
uv run gprof2dot --colour-nodes-by-selftime -f pstats prof | dot -Tpng -o output.png

# line-based profiling
uv run kernprof -l -v 0.py
