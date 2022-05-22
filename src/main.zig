const std = @import("std");

const Graph = struct {
    nodes: []const Node,
    edges: []const usize,
    alloc: std.mem.Allocator,

    const Self = @This();

    const Node = struct {
        start: usize,
        len: usize,
    };

    pub fn format(value: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        return writer.print("{s}{{ .nodes = {{{any}}}, .edges = {{{any}}} ... }}", .{ @typeName(Self), value.nodes, value.edges });
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

    pub fn transpose(self: Self) !Self {
        return self.transpose_alloc(self.alloc);
    }

    const FullEdge = struct {
        from: usize,
        to: usize,
    };
    fn lessThan(_: void, lhs: FullEdge, rhs: FullEdge) bool {
        return (lhs.from < rhs.from);
    }

    pub fn transpose_alloc(self: Self, alloc: std.mem.Allocator) !Self {
        var transposed_edges = try std.ArrayList(FullEdge).initCapacity(alloc, self.edges.len);
        defer transposed_edges.deinit();
        for (self.nodes) |n, from| {
            for (self.out_neighbors_node(n)) |to| {
                transposed_edges.appendAssumeCapacity(FullEdge{ .from = to, .to = from });
            }
        }
        std.sort.sort(FullEdge, transposed_edges.items, {}, lessThan);

        var edges = try alloc.alloc(usize, self.edges.len);
        var nodes = try std.ArrayList(Node).initCapacity(alloc, self.nodes.len);

        var current_from: usize = 0;
        var current_node = Node{ .start = 0, .len = 0 };
        for (transposed_edges.items) |edge, idx| {
            edges[idx] = edge.to;
            if (edge.from == current_from) {
                current_node.len += 1;
            } else {
                nodes.appendAssumeCapacity(current_node);
                while (nodes.items.len < edge.from) {
                    nodes.appendAssumeCapacity(Node{ .start = idx, .len = 0 });
                }
                current_from = edge.from;
                current_node = Node{ .start = idx, .len = 1 };
            }
        }
        nodes.appendAssumeCapacity(current_node);
        while (nodes.items.len < self.nodes.len) {
            nodes.appendAssumeCapacity(Node{ .start = self.nodes.len, .len = 0 });
        }

        return Self{ .nodes = nodes.toOwnedSlice(), .edges = edges, .alloc = alloc };
    }

    pub const Componentization = struct {
        components: []const []const usize,
        alloc: std.mem.Allocator,

        pub fn deinit(self: @This()) void {
            for (self.components) |c| {
                self.alloc.free(c);
            }
            self.alloc.free(self.components);
        }
    };

    // User should call deinit on the result
    // https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    pub fn strongly_connected_components(self: Self) !Componentization {
        return self.strongly_connected_components_alloc(self.alloc);
    }

    pub fn strongly_connected_components_alloc(self: Self, alloc: std.mem.Allocator) !Componentization {
        var postorder = try self.post_order();
        defer alloc.free(postorder);

        var trans = try self.transpose();
        defer trans.deinit();

        var roots = try alloc.alloc(?usize, self.nodes.len);
        defer alloc.free(roots);
        for (roots) |*r| {
            r.* = null;
        }

        const AssignFrame = struct {
            node: usize,
            root: usize,
        };
        var stack = std.ArrayList(AssignFrame).init(alloc);
        defer stack.deinit();

        var i_plus = postorder.len;
        while (i_plus > 0) : (i_plus -= 1) {
            var u = postorder[i_plus - 1];

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

        var result_components = std.ArrayList([]const usize).init(alloc);

        var it = components.iterator();
        while (it.next()) |entry| {
            try entry.value_ptr.append(entry.key_ptr.*);
            try result_components.append(entry.value_ptr.toOwnedSlice());
        }

        return Componentization{
            .components = result_components.toOwnedSlice(),
            .alloc = alloc,
        };
    }

    // Returns a post-order traversal using node 0 as the root
    // Caller owns the returned slice, and should free it using self.alloc
    // https://eli.thegreenplace.net/2015/directed-graph-traversal-orderings-and-applications-to-data-flow-analysis/
    // https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    pub fn post_order(self: Self) ![]const usize {
        return self.post_order_alloc(self.alloc);
    }

    // Caller owns the returned slice, and should free it using the same alloc
    pub fn post_order_alloc(self: Self, alloc: std.mem.Allocator) ![]const usize {
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

        return result.toOwnedSlice();
    }

    // TODO: memory state (regarding component slices) in error cases???
    pub fn condense(self: Self) !Condensation {
        var components = (try self.strongly_connected_components()).components;
        // original node id -> index in components list
        var new_node_id_map = try self.alloc.alloc(?usize, self.nodes.len);
        std.mem.set(?usize, new_node_id_map, null);
        defer self.alloc.free(new_node_id_map);

        var new_node_data = std.ArrayList(Condensation.Node).init(self.alloc);

        for (components) |original_node_ids| {
            var new_node_id = new_node_data.items.len;
            // here is where we take ownership of each component slice from the Componentization
            try new_node_data.append(Condensation.Node{ .component = original_node_ids });
            for (original_node_ids) |original_node_id| {
                new_node_id_map[original_node_id] = new_node_id;
            }
        }
        // we took ownership of the individual slices from the componentization.
        // The top-level slice is left over and needs to be freed
        // todo: maybe this should be done in a defer or errdefer or something?
        self.alloc.free(components);

        // add all trivial dag_nodes to new_node_data
        for (self.nodes) |_, original_node_id| {
            var new_node_id = new_node_data.items.len;
            if (new_node_id_map[original_node_id] == null) {
                try new_node_data.append(Condensation.Node{ .dag_node = original_node_id });
                new_node_id_map[original_node_id] = new_node_id;
            }
        }

        var new_graph_nodes = std.ArrayList(Graph.Node).init(self.alloc);
        var new_graph_edges = std.ArrayList(usize).init(self.alloc);

        var working_merged_edge_set = std.AutoHashMap(usize, void).init(self.alloc);
        defer working_merged_edge_set.deinit();

        for (new_node_data.items) |condensed_node| {
            switch (condensed_node) {
                .dag_node => |orig_idx| {
                    var start = new_graph_edges.items.len;
                    for (self.out_neighbors(orig_idx)) |target| {
                        try new_graph_edges.append(new_node_id_map[target].?);
                    }
                    try new_graph_nodes.append(Node{
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

        return Condensation{
            .graph = Graph{
                .nodes = new_graph_nodes.toOwnedSlice(),
                .edges = new_graph_edges.toOwnedSlice(),
                .alloc = self.alloc,
            },
            .node_data = new_node_data.toOwnedSlice(),
            .alloc = self.alloc,
        };
    }
};

pub const Condensation = struct {
    graph: Graph,
    node_data: []const Node,
    alloc: std.mem.Allocator,

    const Self = @This();
    pub const Node = union(enum) {
        dag_node: usize,
        component: []const usize,

        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(self.component);
        }
    };

    pub fn deinit(self: *Self) void {
        for (self.node_data) |node| {
            if (node == .component) {
                self.alloc.free(node.component);
            }
        }
        self.alloc.free(self.node_data);
        self.graph.deinit();
    }
};

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

    var condensate = try g.condense();
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
        var scc = try g.strongly_connected_components();
        defer scc.deinit();

        try t.expectEqual(scc.components.len, 1);
        try expectEqualSorted(usize, scc.components[0], &[_]usize{ 0, 1, 2 });
    }

    {
        // 0 -> 1 -> 2 -> 3
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{1},
            &[_]usize{2},
            &[_]usize{3},
            &[_]usize{},
        }, alloc);
        var scc = try g.strongly_connected_components();
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.len, 0);
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
        var scc = try g.strongly_connected_components();
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.len, 1);
        try expectEqualSorted(usize, scc.components[0], &[_]usize{ 0, 1, 2 });
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
        var scc = try g.strongly_connected_components();
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.len, 3);
        try expectEqualSorted(usize, scc.components[0], &[_]usize{ 8, 9 });
        try expectEqualSorted(usize, scc.components[1], &[_]usize{ 4, 5, 6 });
        try expectEqualSorted(usize, scc.components[2], &[_]usize{ 0, 1, 2, 3 });
    }

    {
        var g = try Graph.create(&[_][]const usize{
            &[_]usize{1},
            &[_]usize{2},
            &[_]usize{ 3, 4 },
            &[_]usize{0},
            &[_]usize{2},
        }, alloc);
        var scc = try g.strongly_connected_components();
        defer g.deinit();
        defer scc.deinit();

        try t.expectEqual(scc.components.len, 1);
        try expectEqualSorted(usize, scc.components[0], &[_]usize{ 0, 1, 2, 3, 4 });
    }
}
