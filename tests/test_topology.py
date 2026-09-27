"""Unit tests for topology graph building and the traceroute final-hop SNR.

No radio — traceroutes and node lists are plain dicts.
"""
import unittest

from topology import UNKNOWN_HOP, build_graph

ME = "!00000001"


def _node(node_id, name=None, hops=None, snr=None, mqtt=False, heard=1000):
    return {
        "user": {"id": node_id, "longName": name or node_id, "shortName": node_id[-2:]},
        "hopsAway": hops, "snr": snr, "viaMqtt": mqtt, "lastHeard": heard,
    }


def _hop(node_id, snr=None):
    return {"id": node_id, "name": node_id, "snr": snr}


def _trace(to, route, route_back=(), status="ok", ts=1000):
    return {"to": to, "ts": ts, "status": status,
            "route": list(route), "route_back": list(route_back)}


def _links(graph):
    return {(l["source"], l["target"]): l for l in graph["links"]}


class BuildGraphTest(unittest.TestCase):
    def test_multi_hop_trace_becomes_chain_with_receiver_snr(self):
        t = _trace("!00000003",
                   [_hop(ME), _hop("!00000002", 6.0), _hop("!00000003", -2.5)])
        g = build_graph([t], [_node(ME)], my_id=ME)
        links = _links(g)
        self.assertEqual(links[(ME, "!00000002")]["snr"], 6.0)
        self.assertEqual(links[("!00000002", "!00000003")]["snr"], -2.5)
        self.assertEqual(len(links), 2)

    def test_link_keeps_best_snr_across_directions(self):
        t = _trace("!00000002",
                   [_hop(ME), _hop("!00000002", -4.0)],
                   [_hop("!00000002"), _hop(ME, 3.0)])
        g = build_graph([t], [], my_id=ME)
        link = _links(g)[(ME, "!00000002")]
        self.assertEqual(link["snr"], 3.0)
        self.assertEqual(link["seen"], 2)

    def test_only_latest_trace_per_node_counts(self):
        newer = _trace("!00000003", [_hop(ME), _hop("!00000003", 1.0)], ts=2000)
        older = _trace("!00000003",
                       [_hop(ME), _hop("!00000002", 5.0), _hop("!00000003", 2.0)], ts=1000)
        g = build_graph([newer, older], [], my_id=ME)
        self.assertEqual(set(_links(g)), {(ME, "!00000003")})

    def test_unknown_relays_break_the_chain(self):
        t = _trace("!00000003",
                   [_hop(ME), _hop(UNKNOWN_HOP, 4.0), _hop("!00000003", 1.0)])
        g = build_graph([t], [], my_id=ME)
        self.assertEqual(g["links"], [])
        self.assertNotIn(UNKNOWN_HOP, {n["id"] for n in g["nodes"]})

    def test_failed_trace_marks_node_without_links(self):
        g = build_graph([_trace("!00000009", [], status="failed")],
                        [_node(ME), _node("!00000009", hops=3)], my_id=ME)
        failed = next(n for n in g["nodes"] if n["id"] == "!00000009")
        self.assertEqual(failed["trace"], "failed")
        self.assertEqual(g["links"], [])

    def test_directly_heard_rf_nodes_link_to_me(self):
        nodes = [_node(ME), _node("!00000002", hops=0, snr=7.25),
                 _node("!00000003", hops=0, snr=5.0, mqtt=True),
                 _node("!00000004", hops=2, snr=1.0)]
        g = build_graph([], nodes, my_id=ME)
        links = _links(g)
        self.assertEqual(set(links), {(ME, "!00000002")})
        self.assertEqual(links[(ME, "!00000002")]["kinds"], ["heard"])
        # Nodes with no links and no trace stay off the graph.
        self.assertEqual({n["id"] for n in g["nodes"]}, {ME, "!00000002"})

    def test_since_filters_old_traces(self):
        t = _trace("!00000002", [_hop(ME), _hop("!00000002", 1.0)], ts=500)
        g = build_graph([t], [], my_id=ME, since_ts=1000)
        self.assertEqual(g["links"], [])


class FinalSnrTest(unittest.TestCase):
    def test_reads_extra_entry_in_quarter_db(self):
        import mesh_client as mc
        self.assertEqual(mc.MeshClient._final_snr([24, 10], 1), 2.5)

    def test_missing_or_unknown_is_none(self):
        import mesh_client as mc
        self.assertIsNone(mc.MeshClient._final_snr([24], 1))
        self.assertIsNone(mc.MeshClient._final_snr([-128], 0))
        self.assertIsNone(mc.MeshClient._final_snr([], 0))


if __name__ == "__main__":
    unittest.main()
