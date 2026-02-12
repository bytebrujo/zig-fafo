const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public module that dependents can import as "zig-fafo" ---------------
    _ = b.addModule("zig-fafo", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Static library artifact ----------------------------------------------
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "zig-fafo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(lib);

    // Tests ----------------------------------------------------------------
    const test_step = b.step("test", "Run unit tests");

    const root_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(root_tests).step);

    const bloom_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bloom_filter.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(bloom_tests).step);

    const sched_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/scheduler.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_step.dependOn(&b.addRunArtifact(sched_tests).step);
}
