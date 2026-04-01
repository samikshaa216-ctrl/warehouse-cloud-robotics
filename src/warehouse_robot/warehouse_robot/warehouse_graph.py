# warehouse_graph.py

WAREHOUSE_GRAPH = {
    "C1": (0, 0),
    "A1": (2, 0),
    "A2": (4, 0),
    "A3": (6, 0),
    "B1": (0, 3),
    "B2": (2, 3),
    "B3": (4, 3),
    "DZ": (6, 3)   # Drop Zone
}

PATHS = {
    "task_1": ["C1", "A1", "A2", "B2", "DZ"],
    "task_2": ["C1", "B1", "B2", "B3", "DZ"],
    "task_3": ["C1", "A1", "B2", "A3", "DZ"],
    "task_4": ["C1", "A2", "A3", "B3", "DZ"]
}
