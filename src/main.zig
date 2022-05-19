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
        dag_nodes: []const usize,
        alloc: std.mem.Allocator,

        pub fn deinit(self: @This()) !void {
            for (self.components) |c| {
                self.alloc.free(c);
            }
            self.alloc.free(self.components);
            self.alloc.free(self.dag_nodes);
        }
    };

    pub fn post_order(self: Self) !std.ArrayList(usize) {
        return self.post_order_alloc(self.alloc);
    }

    // Returns a post-order traversal using node 0 as the root
    // Caller owns the returned list
    // https://eli.thegreenplace.net/2015/directed-graph-traversal-orderings-and-applications-to-data-flow-analysis/
    // https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    pub fn post_order_alloc(self: Self, alloc: std.mem.Allocator) !std.ArrayList(usize) {
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

        // Owned slice shenanigans in order to free unused capacity. Would be nice if
        // there was just a standard method for this. We could of course just return the
        // slice, but since the caller will have to free it using the proper allocator,
        // I prefer to just hand them the ArrayList with the bundled allocator so they
        // can just call .deinit() to free.
        return std.ArrayList(usize).fromOwnedSlice(alloc, result.toOwnedSlice());
    }

    // User should call deinit on the result
    // https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
    pub fn strongly_connected_components_alloc(self: Self, alloc: std.mem.Allocator) !Componentization {
        var po = try self.post_order();
        defer po.deinit();

        std.log.err("Graph's reverse post-order: {any}", .{po.items});

        return Componentization{
            .components = &[_][]const usize{},
            .dag_nodes = &[_]usize{},
            .alloc = alloc,
        };
    }

    pub fn strongly_connected_components(self: Self) !Componentization {
        return self.strongly_connected_components_alloc(self.alloc);
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
    var t = try g.transpose();
    std.log.info("And its transpose: {any}", .{t});
    _ = try g.strongly_connected_components();
}
