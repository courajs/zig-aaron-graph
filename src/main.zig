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

        std.log.err("component_roots: {any}", .{roots});

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
};

pub fn main() anyerror!void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    var alloc = arena.allocator();

    var g = try Graph.create(&[_][]const usize{
        &[_]usize{1},
        &[_]usize{2},
        &[_]usize{1},
    }, alloc);
    std.log.info("I made this graph: {any}", .{g});

    std.log.info("It's post-order is: {any}", .{g.post_order()});

    std.log.info("Its transpose is: {any}", .{g.transpose()});
}

test "Kosaraju algo for SCCs" {
    const t = std.testing;
    const alloc = t.allocator;

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
    std.log.err("scc: {any}", .{scc});
    try t.expectEqualSlices(usize, scc.components[0], &[_]usize{ 0, 1, 2 });

    // 0 -> 1 -> 2 -> 3
    g.deinit();
    scc.deinit();
    g = try Graph.create(&[_][]const usize{
        &[_]usize{1},
        &[_]usize{2},
        &[_]usize{3},
        &[_]usize{},
    }, alloc);
    scc = try g.strongly_connected_components();

    try t.expectEqual(scc.components.len, 0);
}

// Test cases from:
// https://www.geeksforgeeks.org/tarjan-algorithm-find-strongly-connected-components/
//
//
//
//
//    let g1 = new Graph(5);
//
//    g1.addEdge(1, 0);
//    g1.addEdge(0, 2);
//    g1.addEdge(2, 1);
//    g1.addEdge(0, 3);
//    g1.addEdge(3, 4);
//    document.write("SSC in first graph <br>");
//    g1.SCC();
//
//    let g2 = new Graph(4);
//    g2.addEdge(0, 1);
//    g2.addEdge(1, 2);
//    g2.addEdge(2, 3);
//    document.write("\nSSC in second graph<br> ");
//    g2.SCC();
//
//    let g3 = new Graph(7);
//    g3.addEdge(0, 1);
//    g3.addEdge(1, 2);
//    g3.addEdge(2, 0);
//    g3.addEdge(1, 3);
//    g3.addEdge(1, 4);
//    g3.addEdge(1, 6);
//    g3.addEdge(3, 5);
//    g3.addEdge(4, 5);
//    document.write("\nSSC in third graph <br>");
//    g3.SCC();
//
//    let g4 = new Graph(11);
//    g4.addEdge(0, 1);
//    g4.addEdge(0, 3);
//    g4.addEdge(1, 2);
//    g4.addEdge(1, 4);
//    g4.addEdge(2, 0);
//    g4.addEdge(2, 6);
//    g4.addEdge(3, 2);
//    g4.addEdge(4, 5);
//    g4.addEdge(4, 6);
//    g4.addEdge(5, 6);
//    g4.addEdge(5, 7);
//    g4.addEdge(5, 8);
//    g4.addEdge(5, 9);
//    g4.addEdge(6, 4);
//    g4.addEdge(7, 9);
//    g4.addEdge(8, 9);
//    g4.addEdge(9, 8);
//    document.write("\nSSC in fourth graph<br> ");
//    g4.SCC();
//
//    let g5 = new Graph (5);
//    g5.addEdge(0, 1);
//    g5.addEdge(1, 2);
//    g5.addEdge(2, 3);
//    g5.addEdge(2, 4);
//    g5.addEdge(3, 0);
//    g5.addEdge(4, 2);
//    document.write("\nSSC in fifth graph <br>");
//    g5.SCC();
//
//    // This code is contributed by avanitrachhadiya2155
//
//    </script>
//    Output:
//
//    SCCs in first graph
//    4
//    3
//    1 2 0
//
//    SCCs in second graph
//    3
//    2
//    1
//    0
//
//    SCCs in third graph
//    5
//    3
//    4
//    6
//    2 1 0
//
//    SCCs in fourth graph
//    8 9
//    7
//    5 4 6
//    3 2 1 0
//    10
//
//    SCCs in fifth graph
//    4 3 2 1 0
