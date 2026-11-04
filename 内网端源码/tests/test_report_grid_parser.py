import unittest

from app.modules.shared_bridge.service.report_grid_parser import (
    ReportGridParseError,
    ReportGridParser,
    parse_report_grid,
)


def table(contents):
    return f'<table class="report x-table compact">{contents}</table>'


class ReportGridParserTests(unittest.TestCase):
    def test_vendor_void_element_closing_tags_are_ignored(self):
        html = table('<tr><td><img src="x"></img>测点<br></br>值</td></tr>')
        self.assertEqual(parse_report_grid(html)["grid"], [["测点值"]])

    def test_hidden_point_ids_support_nonzero_prefixes(self):
        html = table('<tr><td hidden id="E1-0">61_0_81_1_10_0</td><td>1</td></tr>')
        self.assertEqual(parse_report_grid(html)["row_ids"], {0: "61_0_81_1_10_0"})

    def test_merged_cells_hidden_ids_entities_and_empty_values(self):
        html = table(
            '<thead><tr><th rowspan="2"> 区域 </th>'
            '<th colspan="2">用电 &amp; <span>能耗</span></th></tr>'
            '<tr><th>今日</th><th>累计</th></tr></thead>'
            '<tbody><tr><td rowspan="2">A</td><td colspan="2"> 42 </td>'
            '<td hidden id="E3-0"><span>0_123_7</span></td></tr>'
            '<tr><td style="color:red; DISPLAY : none !important" id="E4-1" '
            'value="0_456_8"></td><td>33</td><td>&nbsp;</td></tr>'
            '<tr><td>末行</td></tr></tbody>'
        )
        self.assertEqual(
            parse_report_grid(html),
            {
                "grid": [
                    ["区域", "用电 & 能耗", "用电 & 能耗"],
                    ["区域", "今日", "累计"],
                    ["A", "42", "42"],
                    ["A", "33", ""],
                    ["末行", "", ""],
                ],
                "row_ids": {2: "0_123_7", 3: "0_456_8"},
            },
        )

    def test_input_can_be_streamed_one_character_at_a_time(self):
        html = table('<tr><td> 测<b>点</b>&#x1F4A1;&amp;电 </td></tr>')
        expected = {"grid": [["测点💡&电"]], "row_ids": {}}
        self.assertEqual(parse_report_grid(iter(html)), expected)
        parser = ReportGridParser()
        for character in html:
            parser.feed(character)
        self.assertEqual(parser.close(), expected)

    def test_byte_chunks_preserve_split_multibyte_characters(self):
        html = table('<tr><td>中&amp;文😀</td></tr>')
        for encoding in ("utf-8", "gb18030"):
            with self.subTest(encoding=encoding):
                data = html.encode(encoding)
                chunks = (data[i:i + 1] for i in range(len(data)))
                self.assertEqual(
                    parse_report_grid(chunks, encoding=encoding),
                    {"grid": [["中&文😀"]], "row_ids": {}},
                )

    def test_large_stream_with_418_points_and_three_time_columns(self):
        def chunks():
            yield b'<html><body><table class="x-table"><tr><th>point</th><th>t1</th><th>t2</th><th>t3</th></tr>'
            padding = "x" * (45 * 1024)
            for index in range(418):
                yield (
                    f'<tr><td hidden id="E{index}-point">0_{index}_7</td>'
                    f'<td data-padding="{padding}">point-{index}</td>'
                    '<td>1</td><td>2</td><td>3</td></tr>'
                ).encode("utf-8")
            yield b"</table></body></html>"

        result = parse_report_grid(chunks())
        self.assertEqual(len(result["grid"]), 419)
        self.assertEqual(result["row_ids"], {index + 1: f"0_{index}_7" for index in range(418)})
        self.assertEqual(result["grid"][0], ["point", "t1", "t2", "t3"])
        self.assertEqual(result["grid"][-1], ["point-417", "1", "2", "3"])

    def test_point_id_must_be_in_a_hidden_matching_cell(self):
        html = table(
            '<tr><td id="E1-a">0_1_2</td><td hidden id="E2-a">0_2_3</td>'
            '<td hidden id="E3-a">not-an-id</td><td hidden id="X4-a">0_4_5</td>'
            '<td hidden id="E5-">0_5_6</td><td hidden id="E6-a">not_6_7</td>'
            '<td hidden id="E7-a">0_7_bad</td></tr>'
            '<tr><td hidden="false" id="E8-a" value="0_8_9">0_8_9</td>'
            '<td>值</td></tr>'
        )
        self.assertEqual(
            parse_report_grid(html),
            {"grid": [["0_1_2"], ["值"]], "row_ids": {0: "0_2_3", 1: "0_8_9"}},
        )

    def test_hidden_cell_spans_do_not_change_visible_grid(self):
        html = table(
            '<tr><td hidden rowspan="2" colspan="2" id="E1-a">0_1_2</td>'
            '<td>first</td></tr><tr><td>second</td></tr>'
        )
        self.assertEqual(
            parse_report_grid(html),
            {"grid": [["first"], ["second"]], "row_ids": {0: "0_1_2"}},
        )

    def test_inline_display_declarations_and_priority(self):
        html = table(
            '<tr><td style="display:none; display:block">visible</td>'
            '<td style="display:none!important; display:block">hidden</td>'
            '<td style="display:block; DISPLAY: /* comment */ none">hidden</td>'
            '<td style="display:none; display:block!important">visible2</td>'
            '<td style=\'content:"a;display:none;";display:block\'>visible3</td></tr>'
        )
        self.assertEqual(parse_report_grid(html)["grid"], [["visible", "visible2", "visible3"]])

    def test_ambiguous_inline_display_is_rejected(self):
        styles = (
            "display:none;display:var(--visibility)",
            "display:none;display:not-a-display-value",
            r"d\69 splay:none",
            "display:none /* unterminated",
        )
        for style in styles:
            with self.subTest(style=style), self.assertRaises(ReportGridParseError):
                parse_report_grid(table(f'<tr><td style="{style}">x</td></tr>'))

    def test_unrelated_tables_and_non_target_class_are_ignored(self):
        html = (
            '<script>"<table class=x-table>"</script>'
            '<table class="x-table-extra"><tr><td>ignored</td></tr></table>'
            + table('<tr><td>kept</td></tr>')
            + '<table><tr><td>ignored too</td></tr></table>'
        )
        self.assertEqual(parse_report_grid(html)["grid"], [["kept"]])

    def test_empty_table_rows_and_cells(self):
        self.assertEqual(parse_report_grid(table("")), {"grid": [], "row_ids": {}})
        self.assertEqual(
            parse_report_grid(table('<tr></tr><tr><td></td><td><br></td></tr>')),
            {"grid": [["", ""], ["", ""]], "row_ids": {}},
        )

    def test_no_table_multiple_tables_and_nested_tables_are_rejected(self):
        examples = {
            "no table": "<p>no table</p>",
            "wrong class token": '<table class="not-x-table"></table>',
            "two tables": table("") + table(""),
            "second nested target": table('<tr><td>' + table("") + '</td></tr>'),
            "nested other table": table('<tr><td><table></table></td></tr>'),
            "duplicate class": '<table class="x-table" class="other"></table>',
        }
        for case, html in examples.items():
            with self.subTest(case=case), self.assertRaises(ReportGridParseError):
                parse_report_grid(html)

    def test_malformed_table_structure_is_rejected(self):
        examples = {
            "cell outside row": table("<td>x</td>"),
            "row inside cell": table("<tr><td><tr></tr></td></tr>"),
            "row group in row": table("<tr><tbody></tbody></tr>"),
            "nested cell": table("<tr><td><td>x</td></td></tr>"),
            "implicit cell close": table("<tr><td>a<td>b</td></tr>"),
            "missing cell close": table("<tr><td>x</tr>"),
            "missing row close": table("<tr><td>x</td>"),
            "missing table close": '<table class="x-table"><tr><td>x</td></tr>',
            "wrong inline close": table("<tr><td><b>x</td></b></tr>"),
            "self-closing cell": table("<tr><td/></tr>"),
            "self-closing target": '<table class="x-table"/>',
            "duplicate colspan": table('<tr><td colspan="1" colspan="2">x</td></tr>'),
        }
        for case, html in examples.items():
            with self.subTest(case=case), self.assertRaises(ReportGridParseError):
                parse_report_grid(html)

    def test_invalid_spans_are_rejected_even_for_hidden_cells(self):
        values = ("0", "-1", "+1", "1.5", "", "abc", "9" * 5000)
        for name in ("rowspan", "colspan"):
            for value in values:
                for hidden in ("", " hidden"):
                    with self.subTest(name=name, value=value[:10], hidden=hidden):
                        html = table(f'<tr><td{hidden} {name}="{value}">x</td></tr>')
                        with self.assertRaises(ReportGridParseError):
                            parse_report_grid(html)

    def test_overlapping_spans_are_rejected(self):
        html = table(
            '<tr><td>free next row</td><td rowspan="2">occupied</td></tr>'
            '<tr><td colspan="2">overlaps</td></tr>'
        )
        with self.assertRaisesRegex(ReportGridParseError, "overlapping"):
            parse_report_grid(html)

    def test_rowspan_beyond_last_html_row_is_rejected(self):
        with self.assertRaisesRegex(ReportGridParseError, "beyond the last row"):
            parse_report_grid(table('<tr><td rowspan="2">x</td></tr>'))

    def test_rowspan_cannot_cross_html_row_groups(self):
        html = table(
            '<thead><tr><th rowspan="2">header</th></tr></thead>'
            '<tbody><tr><td>data</td></tr></tbody>'
        )
        with self.assertRaisesRegex(ReportGridParseError, "row group"):
            parse_report_grid(html)
        html = table(
            '<tr><td rowspan="2">first</td></tr>'
            '<tbody><tr><td>second</td></tr></tbody>'
        )
        with self.assertRaisesRegex(ReportGridParseError, "row group"):
            parse_report_grid(html)

    def test_conflicting_or_multiple_hidden_point_values_are_rejected(self):
        examples = (
            '<td hidden id="E1-a" value="0_2_3">0_1_2</td>',
            '<td hidden id="E1-a">0_1_2</td><td hidden id="E2-a">0_3_4</td>',
            '<td hidden id="E1-a" id="E2-a">0_1_2</td>',
        )
        for cells in examples:
            with self.subTest(cells=cells), self.assertRaises(ReportGridParseError):
                parse_report_grid(table(f"<tr>{cells}</tr>"))

    def test_row_column_and_expanded_cell_limits(self):
        two_by_two = table("<tr><td>a</td><td>b</td></tr><tr><td>c</td><td>d</td></tr>")
        self.assertEqual(
            parse_report_grid(two_by_two, max_rows=2, max_columns=2, max_cells=4)["grid"],
            [["a", "b"], ["c", "d"]],
        )
        for options, message in (
            ({"max_rows": 1}, "max_rows"),
            ({"max_columns": 1}, "max_columns"),
            ({"max_cells": 3}, "max_cells"),
        ):
            with self.subTest(options=options), self.assertRaisesRegex(ReportGridParseError, message):
                parse_report_grid(two_by_two, **options)
        with self.assertRaisesRegex(ReportGridParseError, "max_cells"):
            parse_report_grid(table('<tr><td rowspan="3" colspan="4">x</td></tr>'), max_cells=11)
        with self.assertRaisesRegex(ReportGridParseError, "max_cells"):
            parse_report_grid(table('<tr><td>x</td></tr><tr></tr>'), max_cells=1)
        with self.assertRaisesRegex(ReportGridParseError, "max_rows"):
            parse_report_grid(table('<tr><td rowspan="2">x</td></tr>'), max_rows=1)

    def test_body_limit_counts_utf8_for_text_and_wire_bytes_for_bytes(self):
        html = table('<tr><td>中文</td></tr>')
        data = html.encode("utf-8")
        self.assertEqual(parse_report_grid(html, max_body_bytes=len(data))["grid"], [["中文"]])
        for chunks in (html, data, [data[:5], data[5:]], list(html)):
            with self.subTest(kind=type(chunks).__name__), self.assertRaisesRegex(
                ReportGridParseError, "max_body_bytes"
            ):
                parse_report_grid(chunks, max_body_bytes=len(data) - 1)

    def test_invalid_limits_and_encoding_are_explicit(self):
        for name in ("max_rows", "max_columns", "max_body_bytes", "max_cells"):
            for value in (0, -1, True, 1.5, "2"):
                with self.subTest(name=name, value=value), self.assertRaisesRegex(
                    ReportGridParseError, name
                ):
                    ReportGridParser(**{name: value})
        with self.assertRaisesRegex(ReportGridParseError, "unknown input encoding"):
            ReportGridParser(encoding="not-a-real-encoding")
        with self.assertRaisesRegex(ReportGridParseError, "invalid input encoding"):
            parse_report_grid([b'<table class="x-table">', b'\xff'])
        with self.assertRaisesRegex(ReportGridParseError, "invalid input encoding"):
            parse_report_grid([b'<table class="x-table"></table>', b'\xe4'])

    def test_mixed_chunks_and_parser_reuse_are_rejected(self):
        with self.assertRaisesRegex(ReportGridParseError, "cannot mix"):
            parse_report_grid(["<table", b' class="x-table"></table>'])
        parser = ReportGridParser()
        parser.feed(table(""))
        parser.close()
        with self.assertRaisesRegex(ReportGridParseError, "already closed"):
            parser.feed("")
        with self.assertRaisesRegex(ReportGridParseError, "already closed"):
            parser.close()
        failed = ReportGridParser()
        with self.assertRaises(ReportGridParseError):
            failed.feed(table("<td>bad</td>"))
        with self.assertRaisesRegex(ReportGridParseError, "failed"):
            failed.close()


if __name__ == "__main__":
    unittest.main()
