---
title: Signals, Events, and runtime Patches
description: Separate requests, observed facts, and live graph changes.
sidebar_position: 3
---

# Signals, Events, and runtime Patches

These three concepts interact, but they are not aliases.

## Signal

A **Signal** is a durable request whose resolution is observable. Triggering a workflow, cancelling by tag, or another supported command may return a Signal identity before its eventual outcome has materialized.

Use the Signal resource to follow that request to its result.

## Event

An **Event** is a lifecycle fact recorded by the Data plane. Events let operators and integrations reconstruct what Tickr observed without turning the event stream into a command interface.

An Event does not imply that every later effect has already completed. Interpret it in the lifecycle stage it names.

## Runtime Patch

A **runtime Patch** is a validated graph operation applied to one live Run. It can grow or reshape the live graph using the public Patch verbs exported by the Core DSL, including chain, fork, branch, expand, swap, cut, prune, trim, and truncate.

A Patch has its own submission and application lifecycle. The Task that submits a Patch must not treat request acceptance as proof that the graph mutation has grounded.

## Definition-time versus runtime composition

- Definition-time composition produces the graph stored in a Workflow definition.
- Runtime Patches operate on the materialized graph of one Run.
- `chain` is the definition-time authoring combinator.
- `mkChain` constructs the runtime Patch verb.

That naming distinction is intentional. Import both through `lib.ncl`; do not import internal DSL modules directly.
