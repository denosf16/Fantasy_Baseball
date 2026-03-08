import pandas as pd
import seaborn as sns
from matplotlib.patches import Rectangle, Polygon, Arc


def draw_zone(ax) -> None:
    zone_left = -0.83
    zone_bottom = 1.5
    zone_width = 1.66
    zone_height = 2.0

    strike_zone = Rectangle(
        (zone_left, zone_bottom),
        zone_width,
        zone_height,
        fill=False,
        linewidth=1.6,
        edgecolor="black",
    )
    ax.add_patch(strike_zone)

    for i in range(1, 3):
        ax.plot(
            [zone_left + i * (zone_width / 3), zone_left + i * (zone_width / 3)],
            [zone_bottom, zone_bottom + zone_height],
            color="black",
            linewidth=0.6,
            alpha=0.45,
        )
        ax.plot(
            [zone_left, zone_left + zone_width],
            [zone_bottom + i * (zone_height / 3), zone_bottom + i * (zone_height / 3)],
            color="black",
            linewidth=0.6,
            alpha=0.45,
        )

    home_plate = Polygon(
        [
            (-0.708, 0.1),
            (0.708, 0.1),
            (0.5, -0.15),
            (0.0, -0.30),
            (-0.5, -0.15),
        ],
        closed=True,
        fill=False,
        linewidth=1.4,
        edgecolor="black",
    )
    ax.add_patch(home_plate)

    ax.set_xlim(2.2, -2.2)
    ax.set_ylim(-0.5, 4.8)
    ax.grid(alpha=0.12)


def draw_kde(ax, df: pd.DataFrame, title: str, min_points: int = 5) -> None:
    draw_zone(ax)

    if df.empty or df["plate_x"].dropna().empty or df["plate_z"].dropna().empty:
        ax.set_title(title, fontsize=10)
        ax.text(
            0.5,
            0.5,
            "No data",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
        )
        return

    if len(df) < min_points or df["plate_x"].nunique() < 2 or df["plate_z"].nunique() < 2:
        ax.set_title(title, fontsize=10)
        ax.text(
            0.5,
            0.5,
            "Not enough data",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=9,
        )
        return

    sns.kdeplot(
        data=df,
        x="plate_x",
        y="plate_z",
        fill=True,
        thresh=0.05,
        levels=20,
        cmap="coolwarm",
        alpha=0.9,
        bw_adjust=1.0,
        warn_singular=False,
        ax=ax,
    )

    draw_zone(ax)
    ax.set_title(title, fontsize=10)


def draw_spray_chart(ax, df_bip: pd.DataFrame, title: str = "Spray Chart") -> None:
    ax.set_title(title)
    ax.set_aspect("equal", adjustable="box")

    infield_arc = Arc(
        (0, 0),
        180,
        180,
        theta1=45,
        theta2=135,
        linewidth=1.2,
        color="black",
    )
    outfield_arc = Arc(
        (0, 0),
        320,
        320,
        theta1=45,
        theta2=135,
        linewidth=1.2,
        color="black",
        alpha=0.6,
    )
    ax.add_patch(infield_arc)
    ax.add_patch(outfield_arc)

    ax.plot([0, -113], [0, 113], color="black", linewidth=1)
    ax.plot([0, 113], [0, 113], color="black", linewidth=1)

    if not df_bip.empty:
        has_hc = {"hc_x", "hc_y"}.issubset(df_bip.columns)
        if has_hc and df_bip["hc_x"].notna().any() and df_bip["hc_y"].notna().any():
            ax.scatter(
                df_bip["hc_x"],
                df_bip["hc_y"],
                s=42,
                alpha=0.65,
                edgecolors="black",
                linewidths=0.25,
            )
        elif "hit_distance_sc" in df_bip.columns and df_bip["hit_distance_sc"].notna().any():
            temp = df_bip.copy()
            n = len(temp)
            temp["plot_x"] = pd.Series(range(n)).apply(
                lambda i: (-1) ** i * (20 + (i % 12) * 8)
            )
            temp["plot_y"] = temp["hit_distance_sc"].fillna(0)
            ax.scatter(
                temp["plot_x"],
                temp["plot_y"],
                s=42,
                alpha=0.65,
                edgecolors="black",
                linewidths=0.25,
            )
        else:
            ax.text(
                0.5,
                0.5,
                "No spray coordinates available",
                transform=ax.transAxes,
                ha="center",
                va="center",
            )
    else:
        ax.text(
            0.5,
            0.5,
            "No balls in play",
            transform=ax.transAxes,
            ha="center",
            va="center",
        )

    ax.set_xlim(-130, 130)
    ax.set_ylim(-10, 330)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.grid(alpha=0.1)