const std = @import("std");

pub const Graph = struct {
    nodes: []const Node,
    edges: []const usize,
    alloc: std.mem.Allocator,

    const Self = @This();
    pub const Node = struct {
        start: usize,
        len: usize,
    };

    pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        return writer.print("{s}{{ .nodes = {any}, .edges = {any} }}", .{ @typeName(Self), value.nodes, value.edges });
    }

    pub fn create(def: []const []const usize, alloc: std.mem.Allocator) !Self {
        var nodes = std.ArrayList(Node).init(alloc);
        var edges = std.ArrayList(usize).init(alloc);
        for (def) |these_edges| {
            try nodes.append(Node{ .start = edges.items.len, .len = these_edges.len });
            try edges.appendSlice(these_edges);
        }
        return Self{
            .nodes = nodes.toOwnedSlice(),
            .edges = edges.toOwnedSlice(),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *Self) void {
        self.alloc.free(self.nodes);
        self.alloc.free(self.edges);
    }

    pub fn out_neighbors(self: Self, n: usize) []const usize {
        return self.out_neighbors_node(self.nodes[n]);
    }
    pub fn out_neighbors_node(self: Self, n: Node) []const usize {
        return self.edges[n.start .. n.start + n.len];
    }
};

const DirectedEdge = struct {
    from: usize,
    to: usize,
};
fn edge_lt_from(_: void, lhs: DirectedEdge, rhs: DirectedEdge) bool {
    return (lhs.from < rhs.from);
}

// Calculate a transposed graph where all edges point the
// opposite direction. Same number of nodes, same number of
// edges, but the structure is different.
pub fn transpose(self: Graph, alloc: std.mem.Allocator) !Graph {
    var reversed_edges = try std.ArrayList(DirectedEdge).initCapacity(alloc, self.edges.len);
    defer reversed_edges.deinit();
    for (self.nodes) |n, from| {
        for (self.out_neighbors_node(n)) |to| {
            // Insert the edge already reversed
            reversed_edges.appendAssumeCapacity(DirectedEdge{ .from = to, .to = from });
        }
    }
    // Sorting groups the edges contiguously by "parent", so we can easily form the
    // node and edge lists
    std.sort.sort(DirectedEdge, reversed_edges.items, {}, edge_lt_from);

    var edges = try alloc.alloc(usize, self.edges.len);
    var nodes = try std.ArrayList(Graph.Node).initCapacity(alloc, self.nodes.len);

    var current_from: usize = 0;
    var current_node = Graph.Node{ .start = 0, .len = 0 };
    for (reversed_edges.items) |edge, idx| {
        edges[idx] = edge.to;
        if (edge.from == current_from) {
            current_node.len += 1;
        } else {
            nodes.appendAssumeCapacity(current_node);
            while (nodes.items.len < edge.from) {
                nodes.appendAssumeCapacity(Graph.Node{ .start = idx, .len = 0 });
            }
            current_from = edge.from;
            current_node = Graph.Node{ .start = idx, .len = 1 };
        }
    }
    nodes.appendAssumeCapacity(current_node);
    while (nodes.items.len < self.nodes.len) {
        nodes.appendAssumeCapacity(Graph.Node{ .start = self.nodes.len, .len = 0 });
    }

    return Graph{ .nodes = nodes.toOwnedSlice(), .edges = edges, .alloc = alloc };
}

// Returns a post-order traversal using node 0 as the root
// https://eli.thegreenplace.net/2015/directed-graph-traversal-orderings-and-applications-to-data-flow-analysis/
// https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
pub fn post_order(self: Graph, alloc: std.mem.Allocator) !std.ArrayList(usize) {
    // Whether we've visited a given node yet while constructing the post-order
    var visited = try alloc.alloc(bool, self.nodes.len);
    defer alloc.free(visited);
    std.mem.set(bool, visited, false);

    var result = try std.ArrayList(usize).initCapacity(alloc, self.nodes.len);

    const DFSFrame = struct {
        node: usize,
        next_out_index: usize,
    };
    // Mark a node visited when we first see it, then add to the post-order once we've
    // visited all children
    var stack = std.ArrayList(DFSFrame).init(alloc);
    defer stack.deinit();
    var i: usize = 0;
    // Since there can be parts of the graph that are completely unconnected
    // to each other, we do have to check every node at the top level
    while (i < self.nodes.len) : (i += 1) {
        // Whenever we find a new unvisited "root", we visit all its out-neighbors recursively
        if (visited[i]) {
            continue;
        }

        visited[i] = true;
        try stack.append(DFSFrame{ .node = i, .next_out_index = 0 });

        while (stack.items.len > 0) {
            var here = &stack.items[stack.items.len - 1];
            // Check remaining out-neighbors of the current stack item.
            for (self.out_neighbors(here.node)[here.next_out_index..]) |out, inc| {
                // this out-neighbor has been visited already via some other path
                if (visited[out]) {
                    continue;
                }
                // otherwise, this is a new node to visit.
                visited[out] = true;
                here.next_out_index += inc + 1;
                try stack.append(DFSFrame{ .node = out, .next_out_index = 0 });
                // having set up the proper stack/visit state, we return to the main stack processing loop
                break;
            } else {
                // else means we've iterated off the end without breaking - this means all out-neighbors have
                // been visited. It's time to add this node to the post-order, and step back up the stack.
                try result.append(here.node);
                _ = stack.pop();
            }
        }
        // We've finished recursively visiting everything reachable from this root.
        // Continue searching for an unvisited root
    }
    // We've now visited each node, and the post-order list is fully populated!

    return result;
}

pub const Componentization = struct {
    components: std.ArrayList(std.ArrayList(usize)),

    pub fn deinit(self: @This()) void {
        for (self.components.items) |comp| {
            comp.deinit();
        }
        self.components.deinit();
    }
};

// const HashSet = std.AutoHashMap(usize, void);
//
// pub fn connected_components(self: Graph, alloc: std.mem.Allocator) !Componentization {
//     var set = HashSet.init(alloc);
//     defer set.deinit();
//     return _connected_components(self, alloc, &set);
// }
// pub fn _connected_components(self: Graph, alloc: std.mem.Allocator, seen: *HashSet) !Componentization {
//     _ = self;
//     _ = alloc;
//     try seen.ensureTotalCapacity(@intCast(HashSet.Size, self.nodes.len));
//     seen.clearRetainingCapacity();
//
//     // loop over every node.
//     // If in seen, skip
//     // If not in seen, do a dfs. All nodes touched are a component. Add each of them to seen
//
//     return Componentization{
//         .components = std.ArrayList(std.ArrayList(usize)).init(alloc),
//     };
// }
//
// test "Connected (not strong) components of a graph" {
//     const t = std.testing;
//     const alloc = t.allocator;
//
//     var g = try Graph.create(&[_][]const usize{
//         &[_]usize{ 3, 4 },
//         &[_]usize{},
//         &[_]usize{4},
//         &[_]usize{2},
//         &[_]usize{},
//     }, alloc);
//     defer g.deinit();
//
//     var components = try connected_components(g, alloc);
//     defer components.deinit();
//
//     try t.expectEqualSlices(usize, components.components.items[0].items, &[_]usize{ 0, 3, 4, 2 });
//     try t.expectEqualSlices(usize, components.components.items[1].items, &[_]usize{1});
// }

// https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
pub fn kosaraju_strong_components(self: Graph, alloc: std.mem.Allocator) !Componentization {
    var postorder = try post_order(self, alloc);
    defer postorder.deinit();

    var trans = try transpose(self, alloc);
    defer trans.deinit();

    var roots = try alloc.alloc(?usize, self.nodes.len);
    defer alloc.free(roots);
    std.mem.set(?usize, roots, null);

    const AssignFrame = struct {
        node: usize,
        root: usize,
    };
    var stack = std.ArrayList(AssignFrame).init(alloc);
    defer stack.deinit();

    // iterate in reverse since this algorithm uses reverse-post-order
    var i_plus = postorder.items.len;
    while (i_plus > 0) : (i_plus -= 1) {
        var u = postorder.items[i_plus - 1];

        if (roots[u] != null) {
            continue;
        }

        try stack.append(AssignFrame{
            .node = u,
            .root = u,
        });
        while (stack.popOrNull()) |frame| {
            if (roots[frame.node] == null) {
                roots[frame.node] = frame.root;
                for (trans.out_neighbors(frame.node)) |n| {
                    try stack.append(AssignFrame{ .node = n, .root = frame.root });
                }
            }
        }
    }

    var components = std.AutoHashMap(usize, std.ArrayList(usize)).init(alloc);
    defer components.deinit();
    for (roots) |r, i| {
        if (r.? != i) {
            var entry = try components.getOrPutValue(r.?, std.ArrayList(usize).init(alloc));
            try entry.value_ptr.append(i);
        }
    }

    var result_components = std.ArrayList(std.ArrayList(usize)).init(alloc);

    var it = components.iterator();
    while (it.next()) |entry| {
        // we skipped adding nodes to their own component to implicitly filter
        // out single-node components. Add it here instead
        try entry.value_ptr.append(entry.key_ptr.*);
        try result_components.append(entry.value_ptr.*);
    }

    return Componentization{
        .components = result_components,
    };
}

// A directed acyclic graph made by condensing any strong components
// in a source possibly-cyclic graph into a single node.
pub const Condensation = struct {
    // The condensed graph, where each node represents either
    // an original node or a strong component of original nodes
    graph: Graph,
    // information about what each dag node corresponds to
    node_data: []const ComponentInfo,
    // mapping from original node index to corresponding
    // dag node
    mapping: []const usize,

    alloc: std.mem.Allocator,

    const Self = @This();
    pub const ComponentInfo = union(enum) {
        dag_node: usize,
        component: []const usize,

        pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
            if (self == .component) {
                alloc.free(self.component);
            }
        }
    };

    pub fn deinit(self: *Self) void {
        self.graph.deinit();
        self.alloc.free(self.mapping);
        for (self.node_data) |node| node.deinit(self.alloc);
        self.alloc.free(self.node_data);
    }

    pub fn count_components(self: Self) usize {
        var count: usize = 0;
        for (self.node_data) |info| {
            if (info == .component) {
                count += 1;
            }
        }
        return count;
    }
};

// TODO: memory state (regarding component slices) in error cases???
pub fn condense_graph(self: Graph, alloc: std.mem.Allocator) !Condensation {
    // map from original graph node id -> corresponding condensed node id
    var new_node_id_map = try alloc.alloc(?usize, self.nodes.len);
    std.mem.set(?usize, new_node_id_map, null);
    defer alloc.free(new_node_id_map);

    // Information about nodes of the condensed graph. Which original nodes are in
    // a given component?
    // Will be owned by the returned Condensation
    var new_node_data = std.ArrayList(Condensation.ComponentInfo).init(alloc);
    errdefer {
        for (new_node_data.items) |dat| dat.deinit(alloc);
        new_node_data.deinit();
    }

    // Each individual component list becomes owned by the returned Condensation,
    // but we do need to free the top-level list containing them.
    var scc = try kosaraju_strong_components(self, alloc);
    errdefer scc.deinit();
    defer scc.components.deinit();

    for (scc.components.items) |*original_node_ids| {
        var new_node_id = new_node_data.items.len;
        for (original_node_ids.items) |original_node_id| {
            new_node_id_map[original_node_id] = new_node_id;
        }
        // here is where we take ownership of each component slice from the Componentization
        try new_node_data.append(Condensation.ComponentInfo{ .component = original_node_ids.toOwnedSlice() });
    }

    // add all trivial dag_nodes to new_node_data
    for (self.nodes) |_, original_node_id| {
        var new_node_id = new_node_data.items.len;
        if (new_node_id_map[original_node_id] == null) {
            try new_node_data.append(Condensation.ComponentInfo{ .dag_node = original_node_id });
            new_node_id_map[original_node_id] = new_node_id;
        }
    }

    var new_graph_nodes = std.ArrayList(Graph.Node).init(self.alloc);
    var new_graph_edges = std.ArrayList(usize).init(self.alloc);
    errdefer new_graph_nodes.deinit();
    errdefer new_graph_edges.deinit();

    var working_merged_edge_set = std.AutoHashMap(usize, void).init(self.alloc);
    defer working_merged_edge_set.deinit();

    for (new_node_data.items) |condensed_node| {
        switch (condensed_node) {
            .dag_node => |orig_idx| {
                var start = new_graph_edges.items.len;
                for (self.out_neighbors(orig_idx)) |target| {
                    try new_graph_edges.append(new_node_id_map[target].?);
                }
                try new_graph_nodes.append(Graph.Node{
                    .start = start,
                    .len = self.nodes[orig_idx].len,
                });
            },
            .component => |orig_ids| {
                working_merged_edge_set.clearRetainingCapacity();
                for (orig_ids) |orig_idx| {
                    for (self.out_neighbors(orig_idx)) |target| {
                        try working_merged_edge_set.put(new_node_id_map[target].?, {});
                    }
                }
                var start = new_graph_edges.items.len;
                var iter = working_merged_edge_set.keyIterator();
                while (iter.next()) |target| {
                    try new_graph_edges.append(target.*);
                }
                try new_graph_nodes.append(Graph.Node{ .start = start, .len = working_merged_edge_set.count() });
            },
        }
    }

    var mapping = try alloc.alloc(usize, new_node_id_map.len);
    errdefer alloc.free(mapping);
    for (new_node_id_map) |node, i| {
        mapping[i] = node.?;
    }

    return Condensation{
        .graph = Graph{
            .nodes = new_graph_nodes.toOwnedSlice(),
            .edges = new_graph_edges.toOwnedSlice(),
            .alloc = self.alloc,
        },
        .node_data = new_node_data.toOwnedSlice(),
        .mapping = mapping,
        .alloc = alloc,
    };
}

test "Graph condensation" {
    const t = std.testing;
    const alloc = t.allocator;

    var g = try Graph.create(&[_][]const usize{
        &[_]usize{1},
        &[_]usize{ 2, 4 },
        &[_]usize{3},
        &[_]usize{0},
        &[_]usize{5},
        &[_]usize{6},
        &[_]usize{7},
        &[_]usize{8},
        &[_]usize{5},
    }, alloc);
    defer g.deinit();

    var condensate = try condense_graph(g, alloc);
    defer condensate.deinit();

    try t.expectEqual(condensate.node_data.len, 3);
    try t.expectEqualSlices(usize, condensate.node_data[0].component, &[_]usize{ 6, 7, 8, 5 });
    try t.expectEqualSlices(usize, condensate.node_data[1].component, &[_]usize{ 1, 2, 3, 0 });
    try t.expectEqual(condensate.node_data[2].dag_node, 4);
}

fn expectEqualSorted(comptime T: type, left: []const T, right: []const T) !void {
    const t = std.testing;
    const sort = std.sort;
    const lt = comptime sort.asc(T);

    var left_s = try t.allocator.dupe(T, left);
    var right_s = try t.allocator.dupe(T, right);
    defer t.allocator.free(left_s);
    defer t.allocator.free(right_s);
    sort.sort(T, left_s, {}, lt);
    sort.sort(T, right_s, {}, lt);

    return t.expectEqualSlices(T, left_s, right_s);
}

// Test cases from:
// https://www.geeksforgeeks.org/tarjan-algorithm-find-strongly-connected-components/
test "Kosaraju algo for SCCs" {
    const t = std.testing;
    const alloc = t.allocator;

    {
        //     0   ->   3  ->  4
        //  ↗️   ↘️
        // 2  <-  1
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{ 2, 3 },
            &[_]usize{0},
            &[_]usize{1},
            &[_]usize{4},
            &[_]usize{},
        }, alloc);
        defer g.deinit();
        var scc = try kosaraju_strong_components(g, alloc);
        defer scc.deinit();

        try t.expectEqual(scc.components.items.len, 1);
        try expectEqualSorted(usize, scc.components.items[0].items, &[_]usize{ 0, 1, 2 });
    }

    {
        // 0 -> 1 -> 2 -> 3
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{1},
            &[_]usize{2},
            &[_]usize{3},
            &[_]usize{},
        }, alloc);
        var scc = try kosaraju_strong_components(g, alloc);
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.items.len, 0);
    }

    {
        //     0
        //  ↗️   ↘️
        // 2   <-  1  ->   6
        //     (dl)  ↘️
        //    3        4
        //      ↘️  (dl)
        //        5
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{1},
            &[_]usize{ 2, 3, 4, 6 },
            &[_]usize{0},
            &[_]usize{5},
            &[_]usize{5},
            &[_]usize{},
            &[_]usize{},
        }, alloc);
        var scc = try kosaraju_strong_components(g, alloc);
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.items.len, 1);
        try expectEqualSorted(usize, scc.components.items[0].items, &[_]usize{ 0, 1, 2 });
    }

    {
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{ 1, 3 },
            &[_]usize{ 2, 4 },
            &[_]usize{ 0, 6 },
            &[_]usize{2},
            &[_]usize{ 5, 6 },
            &[_]usize{ 6, 7, 8, 9 },
            &[_]usize{4},
            &[_]usize{9},
            &[_]usize{9},
            &[_]usize{8},
            &[_]usize{},
        }, alloc);
        var scc = try kosaraju_strong_components(g, alloc);
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.items.len, 3);
        try expectEqualSorted(usize, scc.components.items[0].items, &[_]usize{ 8, 9 });
        try expectEqualSorted(usize, scc.components.items[1].items, &[_]usize{ 4, 5, 6 });
        try expectEqualSorted(usize, scc.components.items[2].items, &[_]usize{ 0, 1, 2, 3 });
    }

    {
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{1},
            &[_]usize{2},
            &[_]usize{ 3, 4 },
            &[_]usize{0},
            &[_]usize{2},
        }, alloc);
        var scc = try kosaraju_strong_components(g, alloc);
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.items.len, 1);
        try expectEqualSorted(usize, scc.components.items[0].items, &[_]usize{ 0, 1, 2, 3, 4 });
    }
}
