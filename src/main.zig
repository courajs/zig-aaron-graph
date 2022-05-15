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

    pub fn out_neighbors(self: Self, n: Node) []const usize {
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
            for (self.out_neighbors(n)) |to| {
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
}
