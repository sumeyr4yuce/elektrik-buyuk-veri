"""Ekip genelinde tutarlı grafik stili (renk sırası sabittir, döngüyle değişmez)."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SERIES = ["#257de0", "#f47230", "#00b894", "#f2a600", "#ee7fac", "#008a17", "#6a4ce2", "#ec4647"]
INK, INK2, GRID = "#191919", "#666666", "#e6e6e6"
SEQ_CMAP = matplotlib.colors.LinearSegmentedColormap.from_list(
    "blue_seq", ["#f2f7fe", "#9dc8f7", "#338ced", "#1757ac", "#0e3d7e"])


def apply():
    plt.rcParams.update({
        "figure.dpi": 130, "savefig.dpi": 150, "savefig.bbox": "tight",
        "figure.facecolor": "white", "axes.facecolor": "white",
        "axes.edgecolor": INK2, "axes.labelcolor": INK2, "axes.titlecolor": INK,
        "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "xtick.color": INK2, "ytick.color": INK2, "font.size": 10,
        "lines.linewidth": 2, "legend.frameon": False, "legend.labelcolor": INK2,
        "axes.prop_cycle": matplotlib.cycler(color=SERIES),
    })
