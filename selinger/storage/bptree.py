class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []

class BPlusTree:
    def __init__(self, t=3):
        self.root = BPlusTreeNode(is_leaf=True)
        self.t = t

    def insert(self, value, record):
        root = self.root
        if len(root.keys) == 2 * self.t - 1:
            temp = BPlusTreeNode()
            self.root = temp
            temp.children.append(root)
            self._split_child(temp, 0)
            self._insert_non_full(temp, value, record)
        else:
            self._insert_non_full(root, value, record)

    def _split_child(self, parent, i):
        t = self.t
        node = parent.children[i]
        new_node = BPlusTreeNode(is_leaf=node.is_leaf)
        parent.children.insert(i + 1, new_node)
        parent.keys.insert(i, node.keys[t - 1])
        
        new_node.keys = node.keys[t: (2 * t - 1)]
        node.keys = node.keys[0: t - 1]
        
        if not node.is_leaf:
            new_node.children = node.children[t: 2 * t]
            node.children = node.children[0: t]

    def _insert_non_full(self, node, value, record):
        if node.is_leaf:
            i = len(node.keys) - 1
            while i >= 0 and value < node.keys[i][0]:
                i -= 1
            node.keys.insert(i + 1, (value, record))
        else:
            i = len(node.keys) - 1
            while i >= 0 and value < node.keys[i][0]:
                i -= 1
            i += 1
            
            if len(node.children[i].keys) == 2 * self.t - 1:
                self._split_child(node, i)
                if value > node.keys[i][0]:
                    i += 1
            self._insert_non_full(node.children[i], value, record)

    def scan(self):
        return self._scan_node(self.root, is_range_scan=False)

    def search(self, value):
        iterator = self._scan_node(self.root, is_range_scan=True, value=value)
        return [(key, record) for key, record in iterator if key == value]

    def range_search(self, low, high):
        return self._scan_node(self.root, is_range_scan=True, low=low, high=high)

    def _scan_node(self, node, is_range_scan, value=None, low=None, high=None):
        if not node:
            return
        i = 0
        while i < len(node.keys):
            if not node.is_leaf:
                yield from self._scan_node(node.children[i], is_range_scan, value, low, high)
            if is_range_scan:
                if value is not None:
                    if node.keys[i][0] == value:
                        yield node.keys[i]
                else:
                    if low <= node.keys[i][0] <= high:
                        yield node.keys[i]
            else:
                yield node.keys[i]
            i += 1
        
        if not node.is_leaf:
            yield from self._scan_node(node.children[i], is_range_scan, value, low, high)
