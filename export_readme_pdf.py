from __future__ import annotations

from pathlib import Path


def main():
    # Lazy import so the script prints a helpful error if reportlab isn't installed.
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.pdfgen import canvas
    except Exception as e:
        raise SystemExit(
            "Missing dependency: reportlab.\n"
            "Install it with:\n"
            "  pip install -r requirements.txt\n"
        ) from e

    root = Path(__file__).resolve().parent
    md_path = root / "README.md"
    pdf_path = root / "README.pdf"

    text = md_path.read_text(encoding="utf-8")

    # Register a basic monospace font if available; fallback to built-in Courier.
    font_name = "Courier"
    try:
        # Windows commonly has Consolas; register if present.
        consolas = Path(r"C:\Windows\Fonts\consola.ttf")
        if consolas.exists():
            pdfmetrics.registerFont(TTFont("Consolas", str(consolas)))
            font_name = "Consolas"
    except Exception:
        pass

    c = canvas.Canvas(str(pdf_path), pagesize=letter)
    width, height = letter

    margin_x = 54  # 0.75 inch
    margin_y = 54
    font_size = 10
    line_height = 12

    c.setTitle("Job Scraping Documentation")
    c.setAuthor("job scrapping workspace")

    c.setFont(font_name, font_size)

    # Simple word-wrap that preserves existing newlines.
    max_width = width - 2 * margin_x

    def wrap_line(line: str) -> list[str]:
        if not line:
            return [""]

        words = line.split(" ")
        out: list[str] = []
        cur = ""
        for w in words:
            candidate = (cur + " " + w).strip() if cur else w
            if c.stringWidth(candidate, font_name, font_size) <= max_width:
                cur = candidate
            else:
                if cur:
                    out.append(cur)
                cur = w
        out.append(cur)
        return out

    y = height - margin_y
    for raw_line in text.splitlines():
        # Keep markdown fences readable in mono output.
        line = raw_line.rstrip("\n")
        for wl in wrap_line(line):
            if y < margin_y:
                c.showPage()
                c.setFont(font_name, font_size)
                y = height - margin_y
            c.drawString(margin_x, y, wl)
            y -= line_height

    c.save()
    print(f"Saved {pdf_path}")


if __name__ == "__main__":
    main()

