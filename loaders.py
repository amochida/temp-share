"""File loading helpers for Docling/TXT based ingestion routes.

Docling を用いたインジェスト処理のうち、ファイルのロードと
「Docling ドキュメント → `_NormalizedElement`」への正規化を担当するモジュール。

主な責務:
- アップロードされたファイルの拡張子に応じて適切な読み込みルートを選択する
- LibreOffice を使った Office 形式（.doc, .docx, .ppt）の変換（PDF / 新形式化）
- Docling による PDF / PPTX の解析と、その結果の正規化
- テキストファイル(.txt)のシンプルな読み込み

このモジュールはあくまで「ファイル → 正規化済み要素」の変換に特化しており、
後続のチャンク化・埋め込み生成などは別レイヤーで行われることを前提としている。
"""

from __future__ import annotations

import os
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling_core.types.doc import DoclingDocument

from backend.foundation.logging import configure_logging
from backend.rag.ingestion.types import (
    DocumentParsingError,
    UnsupportedDocumentTypeError,
    _NormalizedElement,
)

# モジュール専用の logger。
# ここで取得した logger を通じて、変換ルート・件数・エラーなどを一元的に記録する。
logger = configure_logging(__name__)

# PDF の解析時に使用する Docling のパイプラインオプション。
# 現状では「テーブル構造を復元する」ことのみ明示的に有効化している。
_pdf_pipeline_options = PdfPipelineOptions()
_pdf_pipeline_options.do_table_structure = True

# Docling が利用するモデルやアーティファクトのパスを、環境変数から指定できるようにする。
_DOCLING_ARTIFACTS_PATH = os.getenv("DOCLING_ARTIFACTS_PATH")
if _DOCLING_ARTIFACTS_PATH:
    try:
        # PDF パイプラインオプションに artifacts_path を設定。
        # 不正なパスだった場合は警告ログを出してスキップする。
        _pdf_pipeline_options.artifacts_path = _DOCLING_ARTIFACTS_PATH
    except Exception:
        logger.warning(
            "Docling の artifacts_path 設定が無効のためスキップ",
            extra={"path": _DOCLING_ARTIFACTS_PATH},
        )

# Docling の統一コンバータ。
# 現時点では PDF 用のオプションだけを上書きしており、その他の形式は Docling のデフォルト設定に従う。
_DOCLING_CONVERTER = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=_pdf_pipeline_options),
    }
)

# 通常の本文から「見出しらしさ」を判定する際に使用する、日本語の一般的な見出しキーワード。
# 完全一致ではなく「含まれているか」を見ることで、「◯◯の目的」「△△の概要」といった文を見出し候補として扱う。
_GENERIC_HEADER_KEYWORDS = (
    "目的",
    "概要",
    "範囲",
    "活用",
    "方針",
    "計画",
    "対象",
    "体制",
    "留意",
    "関係",
    "定義",
    "手順",
    "背景",
    "課題",
    "位置づけ",
)

# セクション見出しを判定するための正規表現パターン。
# - 「第1章」「第２節」などの日本語見出し
# - 「【〇〇】」形式のブロック見出し
_SECTION_HEADER_PATTERNS = [
    re.compile(r"^第[０-９0-9一二三四五六七八九十百千]+[章節条項目].*"),
    re.compile(r"^【.+?】$"),
]


def load_normalized_elements_from_file(path: Path, suffix: str) -> list[_NormalizedElement]:
    """ファイルパスと拡張子に応じて、正規化済みの要素リストへ変換する。

    ファイルの拡張子 (suffix) に基づき、
    - Office 形式（.doc, .docx）は LibreOffice による PDF 変換 → Docling 解析
    - PDF (.pdf) はそのまま Docling 解析
    - PowerPoint 形式（.pptx）はそのまま Docling 解析
    - 古い PowerPoint (.ppt) は LibreOffice による .pptx 変換 → Docling 解析
    - テキスト (.txt) はシンプルなテキスト読み込み
    といったルートを選択して `_NormalizedElement` のリストを返す。

    ここで返される `_NormalizedElement` は、後続のチャンク化・検索処理から見ると
    「ファイル形式に依存しない中立的な文書要素」となることを意図している。

    Args:
        path (Path): 解析対象となるファイルのパス。
        suffix (str): ファイル拡張子（'.pdf', '.docx' など、先頭にドットを含む小文字想定）。

    Returns:
        list[_NormalizedElement]: 正規化済みのドキュメント要素一覧。

    Raises:
        DocumentParsingError: ファイルの解析・変換中に何らかのエラーが発生した場合。
        UnsupportedDocumentTypeError: サポート対象外の拡張子が指定された場合。
    """
    try:
        if suffix in {".docx", ".doc"}:
            logger.debug(
                "ファイル読み込みルートを実行",
                extra={"suffix": suffix, "path": str(path), "route": "office->pdf->docling"},
            )
            # Word 形式のファイル（.doc / .docx）を LibreOffice 経由で PDF に変換する。
            pdf_path = _convert_office_to_pdf(path, suffix)
            try:
                # 変換済みの PDF を Docling で解析し、正規化された要素リストへ変換する。
                return _load_docling_elements(pdf_path)
            finally:
                try:
                    # 一時的に生成した PDF ファイルを削除してクリーンアップする。
                    pdf_path.unlink(missing_ok=True)
                except Exception:
                    logger.warning(
                        "一時PDFファイルの削除に失敗",
                        extra={"path": str(pdf_path)},
                    )

        if suffix == ".pdf":
            logger.debug(
                "ファイル読み込みルートを実行",
                extra={"suffix": suffix, "path": str(path), "route": "pdf->docling"},
            )
            # 既存の PDF を Docling で解析し、正規化された要素リストへ変換する。
            return _load_docling_elements(path)

        if suffix == ".pptx":
            logger.debug(
                "ファイル読み込みルートを実行",
                extra={"suffix": suffix, "path": str(path), "route": "pptx->docling"},
            )
            # 新形式 PowerPoint(.pptx) を Docling で解析し、正規化された要素リストへ変換する。
            return _load_docling_elements(path)

        if suffix == ".ppt":
            logger.debug(
                "ファイル読み込みルートを実行",
                extra={"suffix": suffix, "path": str(path), "route": "ppt->pptx->docling"},
            )
            # 旧形式 PowerPoint(.ppt) を LibreOffice で .pptx に変換する。
            modern_path = _convert_legacy_office_to_modern(path, suffix)
            try:
                # 変換された .pptx を Docling で解析し、正規化された要素リストへ変換する。
                return _load_docling_elements(modern_path)
            finally:
                try:
                    # 一時的に生成した .pptx ファイルを削除してクリーンアップする。
                    modern_path.unlink(missing_ok=True)
                except Exception:
                    logger.warning(
                        "Office 変換後の一時ファイル削除に失敗",
                        extra={"path": str(modern_path)},
                    )

        if suffix == ".txt":
            logger.debug(
                "ファイル読み込みルートを実行",
                extra={"suffix": suffix, "path": str(path), "route": "txt"},
            )
            # テキストファイルを読み込み、1 つの NarrativeText エレメントとして返す。
            return _load_txt_elements(path)

    except DocumentParsingError:
        # すでに意味のあるメッセージにラップしているので、そのまま上位に投げる。
        raise
    except Exception as exc:
        # どのルートでも想定外のエラーが発生した場合は、共通のパースエラーとして扱う。
        logger.exception("ファイルの正規化処理に失敗", extra={"suffix": suffix, "path": str(path)})
        raise DocumentParsingError("ファイルの解析に失敗しました。内容を確認してください。") from exc

    # 上記のいずれの条件にもマッチしなかった場合は、拡張子が未サポートと判断する。
    logger.debug(
        "サポートされていないアップロード形式を検出",
        extra={"suffix": suffix, "path": str(path)},
    )
    raise UnsupportedDocumentTypeError(f"未対応のファイル形式です: {suffix}")


def _load_docling_elements(path: Path) -> list[_NormalizedElement]:
    """Docling を用いてファイルを解析し、正規化された要素リストに変換する。

    与えられたパスのファイルを Docling の `DocumentConverter` に渡し、
    生成された `DoclingDocument` から `_NormalizedElement` のリストを構築して返す。

    Args:
        path (Path): Docling による解析対象ファイルのパス。

    Returns:
        list[_NormalizedElement]: Docling の解析結果をベースに正規化されたドキュメント要素一覧。

    Raises:
        DocumentParsingError: Docling の変換処理でエラーが発生した場合。
    """
    logger.debug("Docling による変換を開始", extra={"path": str(path)})
    try:
        # Docling のコンバータでファイルを解析し、中間表現の DoclingDocument を得る。
        result = _DOCLING_CONVERTER.convert(str(path))
        doc: DoclingDocument = result.document
    except Exception as exc:
        logger.exception("Docling による変換に失敗", extra={"path": str(path)})
        raise DocumentParsingError("ファイルの解析に失敗しました。内容を確認してください。") from exc

    # DoclingDocument から `_NormalizedElement` のリストへ変換（正規化）する。
    elements = _normalize_docling_document(doc)
    logger.debug(
        "Docling による変換が完了",
        extra={"path": str(path), "element_count": len(elements)},
    )
    # エレメント種別ごとの分布をログに出すことで、デバッグ時に構造の偏りを確認しやすくする。
    type_counts = Counter(elem.type for elem in elements)
    logger.debug(
        "Docling から正規化したエレメントの統計",
        extra={
            "element_count": len(elements),
            "type_counts": dict(type_counts),
        },
    )
    return elements


def _normalize_docling_document(doc: DoclingDocument) -> list[_NormalizedElement]:
    """DoclingDocument から `_NormalizedElement` のリストへ変換する。

    Docling が提供する `doc.iterate_items()` を用いてドキュメント内のノードを走査し、
    - ラベル（label）や content_layer に基づくフィルタリング
    - 見出し・本文・リスト・表などの型分類
    - ページ番号の取得（可能な場合）
    を行った上で、後続処理に適した `_NormalizedElement` に正規化する。

    ここでは「どのノードを保持し、どのノードを捨てるか」を決めるロジックが集約されており、
    解析結果の粒度やノイズの多少に直結する重要な処理となる。

    Args:
        doc (DoclingDocument): Docling により解析済みのドキュメントオブジェクト。

    Returns:
        list[_NormalizedElement]: 正規化されたドキュメント要素の一覧。
    """
    # 最終的に返す `_NormalizedElement` のリスト。
    elements: list[_NormalizedElement] = []
    # ドキュメント内での出現順を示すシーケンシャルな番号。
    order = 0
    content_layer_counts = Counter()

    # Docling のノードを順番に走査する。
    # iterate_items() からは (node, parent) のようなタプルが返ってくるが、
    # ここでは node のみを利用する。
    for node, _ in doc.iterate_items():
        # Docling が付与したラベルオブジェクト（または文字列）を取得。
        label_raw = getattr(node, "label", None)
        # ラベル表現を小文字の文字列に正規化する。
        label_str = _normalize_docling_label(label_raw)
        if not label_str:
            # ラベルが無いノードもテキストがあれば拾う（ノイズ時はこの2行をコメントアウトして戻せる）。
            fallback_text = getattr(node, "text", "") or ""
            if not fallback_text.strip():
                continue
            label_str = "unknown"
            text = fallback_text
            elem_type = "NarrativeText"
            prov = getattr(node, "prov", []) or []
            page_no = None
            if prov:
                first = prov[0]
                page_no = getattr(first, "page_no", None) or getattr(first, "page", None)
            page = _normalize_page(page_no)
            metadata: dict[str, Any] = {
                "source": "docling",
                "label": label_str,
                "content_layer": str(getattr(node, "content_layer", None)),
                "_order": order,
            }
            order += 1
            elements.append(
                _NormalizedElement(
                    type=elem_type,
                    text=text.strip(),
                    page=page,
                    metadata=metadata,
                )
            )
            continue

        # content_layer（本文、ヘッダ、フッタ等のレイヤ）を取得して、
        # 本文以外のレイヤを原則として除外する。
        content_layer = getattr(node, "content_layer", None)
        content_layer_counts[str(content_layer).lower() if content_layer is not None else "none"] += 1
        # できる限り拾うため、content_layer ではフィルタしない。
        # ノイズ除去したい場合は以下を有効化:
        # if content_layer is not None:
        #     cl = str(content_layer).lower()
        #     if all(key not in cl for key in ("body", "header", "footer")):
        #         continue

        # 画像はここではテキストとして扱わないため除外する。
        if label_str in {"picture"}:
            continue

        # 後続の分岐で値を設定するための初期値。
        text = ""
        # デフォルトは本文テキスト扱い（NarrativeText）とする。
        elem_type = "NarrativeText"

        # ラベル種別ごとにテキストの取り出し方とエレメント種別を決定する。
        if label_str in {
            "section_header",
            "title",
            "text",
            "list_item",
            "caption",
            "code",
            "formula",
        }:
            # これらのラベルは `.text` プロパティにプレーンテキストが入っている想定。
            text = getattr(node, "text", "") or ""
            if not text.strip():
                # 空文字または空白のみの場合はノイズとしてスキップ。
                continue

            # ラベルに応じて `_NormalizedElement.type` を割り当てる。
            if label_str == "section_header":
                elem_type = "SectionHeader"
            elif label_str == "title":
                elem_type = "Title"
            elif label_str == "list_item":
                elem_type = "ListItem"
            else:
                # text / caption / code / formula などは一旦 NarrativeText として扱う。
                elem_type = "NarrativeText"

        elif label_str == "table":
            # 表（table）は export_to_markdown を使って Markdown 文字列として取り出す。
            try:
                text = node.export_to_markdown(doc=doc)
            except Exception:
                # 変換に失敗した場合は fallback として `.text` を参照する。
                text = getattr(node, "text", "") or ""
            if not text.strip():
                continue
            elem_type = "Table"

        else:
            # その他のラベルについても `.text` にある程度の文字列が入ることが多いので、
            # 空でなければ NarrativeText として扱う。
            text = getattr(node, "text", "") or ""
            if not text.strip():
                continue
            elem_type = "NarrativeText"

        # 前後の空白をまとめて除去し、きれいなテキストとして扱う。
        text = text.strip()

        # 一旦 NarrativeText と判定した要素でも、
        # 内容が見出しパターンに近い場合は SectionHeader に昇格させる。
        if elem_type == "NarrativeText":
            if _looks_like_section_header(text) or _looks_like_generic_header(text):
                elem_type = "SectionHeader"

        # provenance 情報（どのページ・どの領域から抽出されたか）を取得する。
        prov = getattr(node, "prov", []) or []
        page_no = None
        if prov:
            first = prov[0]
            # Docling のバージョンやパイプラインにより属性名が異なる可能性があるため
            # page_no / page の両方を順に試している。
            page_no = getattr(first, "page_no", None) or getattr(first, "page", None)
        # ページ番号を int に正規化し、取得できなかった場合は 1 ページ目として扱う。
        page = _normalize_page(page_no)

        # 後続の検索・デバッグで便利なメタ情報をまとめる。
        metadata: dict[str, Any] = {
            "source": "docling",
            "label": label_str,
            "content_layer": str(content_layer) if content_layer is not None else None,
            # ドキュメント内での出現順。全文検索結果を元の順序に戻す時などに利用できる。
            "_order": order,
        }
        order += 1

        # 最終的な正規化済みエレメントを生成してリストに追加する。
        elements.append(
            _NormalizedElement(
                type=elem_type,
                text=text,
                page=page,
                metadata=metadata,
            )
        )

    logger.debug(
        "Docling content_layer 分布",
        extra={"content_layers": dict(content_layer_counts)},
    )
    return elements


def _normalize_docling_label(label: Any) -> str:
    """Docling のラベル表現を小文字の文字列として正規化する。

    Docling のノードラベルは、
    - 列挙型オブジェクト（value や name 属性を持つ）
    - 生の文字列
    - その他のオブジェクト
    など、複数のパターンになり得るため、ここで一度文字列に正規化しておく。

    Args:
        label (Any): Docling ノードの label プロパティの値。

    Returns:
        str: 小文字に正規化されたラベル文字列。変換できない場合は空文字を返す。
    """
    if label is None:
        return ""
    # Enum などの場合に value/name 属性を優先して文字列化する。
    for attr in ("value", "name"):
        v = getattr(label, attr, None)
        if isinstance(v, str):
            return v.lower()
    # すでに文字列であれば小文字にして返す。
    if isinstance(label, str):
        return label.lower()
    # それ以外の型については str() で文字列表現に変換してから小文字化する。
    return str(label).lower()


def _normalize_page(page: Any | None) -> int:
    """ページ番号を整数に正規化し、取得できない場合は 1 ページ目とみなす。

    Docling の provenance 情報には、ページ番号が
    - None
    - 文字列
    - 数値
    など様々な形で入っている可能性があるため、ここで一度 int に正規化する。

    現状の実装では、ページ番号が不明／パースできない場合に 1 を返す設計になっている。
    そのため、ページを持たないフォーマット（例: DOCX のフローモデル）でも
    すべて 1 ページ扱いになる点に注意が必要。

    Args:
        page (Any | None): Docling の provenance 由来のページ情報。

    Returns:
        int: 1 以上の整数ページ番号。取得できない場合は 1。
    """
    try:
        if page is None:
            return 1
        page_int = int(page)
        return max(page_int, 1)
    except (TypeError, ValueError):
        return 1


def _looks_like_section_header(text: str) -> bool:
    """文字列がセクション見出しらしいかどうかを、パターンマッチで判定する。

    「第◯章」「第◯節」や「【〜】」形式など、見た目だけで見出しと判断できるパターンを
    正規表現でチェックする。

    Args:
        text (str): 判定対象の文字列。

    Returns:
        bool: セクション見出しらしいと判定できれば True、そうでなければ False。
    """
    stripped = text.strip()
    if not stripped:
        return False
    return any(pat.match(stripped) for pat in _SECTION_HEADER_PATTERNS)


def _looks_like_generic_header(text: str) -> bool:
    """文字列が「一般的な見出し」らしいかどうかを、複数のヒューリスティックで判定する。

    概ね次のような条件をすべて満たす場合に「見出しっぽい」とみなす:

    - セクション見出しパターン（第◯章 / 【〜】）に一致する、または
      特定のキーワード（目的, 概要 など）が含まれている
    - 文字数が長すぎない（40 文字以下）
    - 文末に句点や感嘆符が含まれていない
    - スペースが多すぎない（単なる長い文章ではない）

    Args:
        text (str): 判定対象の文字列。

    Returns:
        bool: 一般的な見出しらしいと判断できる場合は True、それ以外は False。
    """
    stripped = text.strip()
    if not stripped:
        return False
    # まずは明らかな見出しパターン（第◯章／【〜】）に一致するかを確認。
    if any(pat.match(stripped) for pat in _SECTION_HEADER_PATTERNS):
        return True
    # あまり長い場合は見出しではなく本文とみなす。
    if len(stripped) > 40:
        return False
    # 日本語の句点・感嘆符が含まれている場合、文としての可能性が高い。
    if re.search(r"[。！？]", stripped):
        return False
    # 空白が多い（単語数が多い）場合も本文の可能性が高い。
    if re.search(r"\s", stripped) and stripped.count(" ") + stripped.count("　") > 5:
        return False
    # 代表的な見出しキーワードが含まれていれば見出し候補とする。
    return any(keyword in stripped for keyword in _GENERIC_HEADER_KEYWORDS)


def _convert_office_to_pdf(path: Path, suffix: str) -> Path:
    """Word 形式ファイル（.doc, .docx）を LibreOffice を使って PDF に変換する。

    Docling は .docx を直接扱えるが、ページ番号を取得するために PDF に変換している。
    また、.doc 形式は Docling が直接サポートしていないため、PDF 変換が必須となる。

    LibreOffice (soffice / libreoffice) コマンドをヘッドレスモードで起動し、
    指定された出力ディレクトリに PDF を生成する。変換の失敗やタイムアウト時には
    `DocumentParsingError` を送出する。

    Args:
        path (Path): 変換対象の .doc / .docx ファイルのパス。
        suffix (str): ファイル拡張子（'.doc' または '.docx'）。

    Returns:
        Path: 生成された PDF ファイルのパス。

    Raises:
        ValueError: .doc / .docx 以外の拡張子が渡された場合。
        DocumentParsingError: LibreOffice による変換が失敗した場合。
    """
    if suffix not in {".doc", ".docx"}:
        raise ValueError(f"unsupported suffix for PDF conversion: {suffix}")

    # 変換後の PDF の出力先を、入力ファイルと同じディレクトリにする。
    outdir = path.parent
    # 環境によってコマンド名が異なる可能性があるため、soffice / libreoffice の両方を試す。
    cmd_candidates = [["soffice"], ["libreoffice"]]
    last_error: Exception | None = None

    # 日本語環境での文字化けを防ぐため、ロケール系環境変数を明示的に設定する。
    env = os.environ.copy()
    env.setdefault("LANG", "ja_JP.UTF-8")
    env.setdefault("LC_CTYPE", "ja_JP.UTF-8")
    env.setdefault("LC_ALL", "ja_JP.UTF-8")

    # LibreOffice のユーザープロファイルディレクトリを環境変数から指定できるようにする。
    profile_dir = os.getenv("LIBREOFFICE_USER_PROFILE_DIR")
    profile_arg: list[str] = []
    if profile_dir:
        try:
            profile_url = Path(profile_dir).resolve().as_uri()
            # LibreOffice の -env:UserInstallation 引数でプロファイルを指定。
            profile_arg = [f"-env:UserInstallation={profile_url}"]
        except Exception:
            logger.warning(
                "LIBREOFFICE_USER_PROFILE_DIR の指定が無効のため無視",
                extra={"profile_dir": profile_dir},
            )

    # LibreOffice の convert-to 用引数。
    # PDF バージョンを SelectPdfVersion オプションで指定している。
    convert_to = 'pdf:writer_pdf_Export:{"SelectPdfVersion":{"type":"long","value":1}}'

    # 利用可能なコマンド候補（soffice / libreoffice）を順に試す。
    for base in cmd_candidates:
        cmd = base + profile_arg + [
            "--headless",
            "--invisible",
            "--nologo",
            "--norestore",
            "--nodefault",
            "--nolockcheck",
            "--nofirststartwizard",
            "--convert-to",
            convert_to,
            "--outdir",
            str(outdir),
            str(path),
        ]
        try:
            # LibreOffice をサブプロセスとして起動し、タイムアウトも設定しておく。
            completed = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                env=env,
                timeout=120,
            )
        except FileNotFoundError as exc:
            # コマンド自体が見つからなかった場合は、次の候補を試す。
            last_error = exc
            continue
        except subprocess.TimeoutExpired as exc:
            # 変換がタイムアウトした場合は警告ログを出しつつ、次の候補を試す。
            last_error = exc
            logger.warning(
                "LibreOffice による PDF 変換がタイムアウト",
                extra={"path": str(path), "cmd": cmd},
            )
            continue

        if completed.returncode != 0:
            # LibreOffice が異常終了した場合は stderr を含めてログを残し、次の候補を試す。
            last_error = RuntimeError(
                f"conversion failed: {' '.join(cmd)}; "
                f"stderr={completed.stderr.decode(errors='ignore')}"
            )
            logger.warning(
                "LibreOffice による PDF 変換候補が失敗",
                extra={
                    "path": str(path),
                    "returncode": completed.returncode,
                },
            )
            continue

        # 期待される PDF ファイル名（拡張子だけ .pdf にしたもの）をまず探す。
        candidate = path.with_suffix(".pdf")
        if candidate.exists():
            return candidate

        # 直接の候補名が見つからない場合、出力ディレクトリ内の PDF を走査し、
        # 元ファイルと同じ stem を持つものを探す。
        for p in outdir.glob("*.pdf"):
            if p.stem == path.stem:
                return p

        # ここまで到達した場合、変換は成功したがファイルが見つからない状態。
        last_error = RuntimeError("converted PDF file not found after soffice call")

    # すべての候補コマンドが失敗した場合、ユーザーに環境依存のエラーであることを伝える。
    msg = (
        "Word ファイル（.doc / .docx）の PDF 変換に失敗しました。"
        "サーバーに LibreOffice(soffice) / libreoffice がインストールされているか確認するか、"
        "あらかじめ PDF へ変換してからアップロードしてください。"
    )
    if last_error:
        logger.exception(
            "Word から PDF への変換に失敗",
            extra={"path": str(path)},
        )
    raise DocumentParsingError(msg)


def _convert_legacy_office_to_modern(path: Path, suffix: str) -> Path:
    """旧形式 PowerPoint ファイル（.ppt）を LibreOffice で .pptx に変換する。

    Docling は .pptx（Office Open XML 形式）をサポートしている一方、
    古いバイナリ形式である .ppt は直接扱えないため、事前に .pptx へ変換する。

    Args:
        path (Path): 変換対象の .ppt ファイルのパス。
        suffix (str): 拡張子（'.ppt' のみをサポート）。

    Returns:
        Path: 生成された .pptx ファイルのパス。

    Raises:
        ValueError: .ppt 以外の拡張子が渡された場合。
        DocumentParsingError: LibreOffice による変換が失敗した場合。
    """
    if suffix == ".ppt":
        target_ext = ".pptx"
        convert_to_arg = "pptx"
    else:
        raise ValueError(f"unsupported legacy suffix for conversion: {suffix}")

    # 変換後の .pptx の出力先を、入力ファイルと同じディレクトリにする。
    outdir = path.parent
    cmd_candidates = [["soffice"], ["libreoffice"]]
    last_error: Exception | None = None

    # 日本語環境での文字化けを防ぐためにロケールを設定する。
    env = os.environ.copy()
    env.setdefault("LANG", "ja_JP.UTF-8")
    env.setdefault("LC_CTYPE", "ja_JP.UTF-8")

    # soffice / libreoffice のいずれかで変換を試みる。
    for base in cmd_candidates:
        cmd = base + [
            "--headless",
            "--convert-to",
            convert_to_arg,
            "--outdir",
            str(outdir),
            str(path),
        ]
        try:
            completed = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                env=env,
            )
        except FileNotFoundError as exc:
            # 該当コマンドがインストールされていない場合は、次の候補へ。
            last_error = exc
            continue

        if completed.returncode != 0:
            # 変換がエラー終了した場合は詳細を last_error に退避し、別候補を試す。
            last_error = RuntimeError(
                f"conversion failed: {' '.join(cmd)}; "
                f"stderr={completed.stderr.decode(errors='ignore')}"
            )
            continue

        # 期待される .pptx のファイルパスをまずは探す。
        candidate = path.with_suffix(target_ext)
        if candidate.exists():
            return candidate

        # 直接の候補が見つからない場合、出力ディレクトリ内の .pptx を走査。
        for p in outdir.glob(f"*{target_ext}"):
            if p.stem == path.stem:
                return p

        # ここに到達した場合、変換は成功した様子だがファイルが見当たらない。
        last_error = RuntimeError("converted file not found after soffice call")

    # すべての候補が失敗した場合は、ユーザーに変換の失敗を知らせる。
    msg = (
        "古い PowerPoint 形式ファイル（.ppt）の変換に失敗しました。"
        "サーバーに LibreOffice(soffice) / libreoffice がインストールされているか確認するか、"
        "あらかじめ .pptx へ変換してからアップロードしてください。"
    )
    if last_error:
        logger.exception(
            "旧形式 PowerPoint の変換に失敗",
            extra={"path": str(path)},
        )
    raise DocumentParsingError(msg)


def _load_txt_elements(path: Path) -> list[_NormalizedElement]:
    """プレーンテキストファイルを読み込み、単一の NarrativeText 要素として返す。

    現状の実装では、テキスト全体を 1 つの `_NormalizedElement` にまとめており、
    行単位・段落単位での分割は行っていない。長大な .txt の場合は、
    後続のチャンク化処理で適切に分割される前提となっている。

    Args:
        path (Path): 読み込み対象のテキストファイルパス。

    Returns:
        list[_NormalizedElement]: 単一の NarrativeText 要素を要素とするリスト。
    """
    try:
        # UTF-8 で読み込みを試みる。
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # 一部のファイルでエンコーディングエラーが出た場合、
        # 不正なバイトを無視して読み込みを継続する。
        content = path.read_text(encoding="utf-8", errors="ignore")

    # ファイル全体の前後の空白を除去。
    content = content.strip()
    if not content:
        # 中身が空の場合はエレメントを生成せず空リストを返す。
        return []

    # テキスト由来であることを示すメタ情報。
    meta = {"source": "txt", "type": "NarrativeText", "_order": 0}
    logger.debug(
        "テキストファイルの読み込みが完了",
        extra={
            "path": str(path),
            "char_length": len(content),
        },
    )
    # テキスト全体を 1 つの NarrativeText エレメントとして返す。
    return [
        _NormalizedElement(
            type="NarrativeText",
            text=content,
            page=1,
            metadata=meta,
        )
    ]


__all__ = ["load_normalized_elements_from_file"]
