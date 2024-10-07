from typing import Any, List, Optional, Tuple

class BPlusNode:
    def __init__(self, leaf: bool = False):
        self.leaf = leaf
        self.keys: List[Any] = []
        self.children: List[BPlusNode] = []
        self.next: Optional[BPlusNode] = None

class BPlusTree:
    def __init__(self, order: int):
        self.root = BPlusNode(leaf=True)
        self.order = order

    def insert(self, key: Any, value: Any):
        if len(self.root.keys) == (2 * self.order) - 1:
            new_root = BPlusNode()
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node: BPlusNode, key: Any, value: Any):
        i = len(node.keys) - 1
        if node.leaf:
            node.keys.append((None, None))
            while i >= 0 and key < node.keys[i][0]:
                node.keys[i + 1] = node.keys[i]
                i -= 1
            node.keys[i + 1] = (key, value)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == (2 * self.order) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent: BPlusNode, index: int):
        order = self.order
        child = parent.children[index]
        new_child = BPlusNode(leaf=child.leaf)

        parent.keys.insert(index, child.keys[order - 1])
        parent.children.insert(index + 1, new_child)

        new_child.keys = child.keys[order:]
        child.keys = child.keys[:order - 1]

        if not child.leaf:
            new_child.children = child.children[order:]
            child.children = child.children[:order]
        else:
            new_child.next = child.next
            child.next = new_child

    def search(self, key: Any) -> Optional[Any]:
        return self._search(self.root, key)

    def _search(self, node: BPlusNode, key: Any) -> Optional[Any]:
        i = 0
        while i < len(node.keys) and key > node.keys[i][0]:
            i += 1
        if node.leaf:
            if i < len(node.keys) and node.keys[i][0] == key:
                return node.keys[i][1]
            return None
        return self._search(node.children[i], key)

    def range_query(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        result = []
        leaf = self._find_leaf(self.root, start_key)
        while leaf:
            for key, value in leaf.keys:
                if start_key <= key <= end_key:
                    result.append((key, value))
                elif key > end_key:
                    return result
            leaf = leaf.next
        return result

    def _find_leaf(self, node: BPlusNode, key: Any) -> BPlusNode:
        if node.leaf:
            return node
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)
