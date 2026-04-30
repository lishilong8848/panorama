from __future__ import annotations

import ast
from typing import Dict, Set


class ExpressionError(ValueError):
    pass


_ALLOWED_BIN_OPS = (ast.Add, ast.Sub, ast.Mult, ast.Div)
_ALLOWED_UNARY_OPS = (ast.UAdd, ast.USub)
_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.operator,
    ast.unaryop,
    ast.Name,
    ast.Load,
    ast.Constant,
)


def _to_number(value: object) -> float:
    if isinstance(value, bool):
        raise ExpressionError("布尔值不能用于表达式计算")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip().replace(",", "")
    if not text:
        raise ExpressionError("变量为空")
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except ValueError as exc:
        raise ExpressionError(f"无法转换为数字: {value}") from exc


def _parse(expression: str) -> ast.Expression:
    expr = str(expression or "").strip()
    if not expr:
        raise ExpressionError("表达式为空")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"表达式语法错误: {exc}") from exc
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ExpressionError(f"不支持的表达式节点: {type(node).__name__}")
        if isinstance(node, ast.BinOp) and not isinstance(node.op, _ALLOWED_BIN_OPS):
            raise ExpressionError(f"不支持的运算符: {type(node.op).__name__}")
        if isinstance(node, ast.UnaryOp) and not isinstance(node.op, _ALLOWED_UNARY_OPS):
            raise ExpressionError(f"不支持的一元运算符: {type(node.op).__name__}")
    return tree


def get_expression_variables(expression: str) -> Set[str]:
    tree = _parse(expression)
    variables: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            variables.add(node.id)
    return variables


def _eval_node(node: ast.AST, variables: Dict[str, object]) -> float:
    if isinstance(node, ast.Constant):
        return _to_number(node.value)

    if isinstance(node, ast.Name):
        if node.id not in variables:
            raise ExpressionError(f"未定义变量: {node.id}")
        return _to_number(variables[node.id])

    if isinstance(node, ast.UnaryOp):
        value = _eval_node(node.operand, variables)
        if isinstance(node.op, ast.UAdd):
            return value
        if isinstance(node.op, ast.USub):
            return -value
        raise ExpressionError(f"不支持的一元运算: {type(node.op).__name__}")

    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, variables)
        right = _eval_node(node.right, variables)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            if right == 0:
                raise ExpressionError("除数不能为0")
            return left / right
        raise ExpressionError(f"不支持的二元运算: {type(node.op).__name__}")

    raise ExpressionError(f"不支持的表达式结构: {type(node).__name__}")


def evaluate_expression(expression: str, variables: Dict[str, object]) -> float:
    tree = _parse(expression)
    return _eval_node(tree.body, variables)
