import sys

import typer

from xrpa_core.command.ads.ads_commands import ads
from xrpa_core.command.apply.sub_command import application
from xrpa_core.command.bd.sub_command import bd
from xrpa_core.command.env.sub_command import env
from xrpa_core.command.inventory.sub_command import inventory
from xrpa_core.command.logistics.sub_command import logistics
from xrpa_core.command.order.order_commands import order
from xrpa_core.command.tools.sub_command import tools
from xrpa_core.command.video.sub_command import video
from xrpa_core.core.logger import logger
from xrpa_core.crawler.api.base import UnauthorizedError
from xrpa_core.feishu.feishu_notify import feishu_exception_notify, feishu_notify_by_key

app = typer.Typer(
    help="TK Automation",
    no_args_is_help=True,
)
app.add_typer(logistics, name="logistics")
app.add_typer(application, name="apply")
app.add_typer(bd, name="bd")
app.add_typer(tools, name="tools")
app.add_typer(video, name="video")
app.add_typer(env, name="env")
app.add_typer(order, name="order")
app.add_typer(ads, name="ads")
app.add_typer(inventory, name="inventory")


if __name__ == "__main__":
    cmd_args = " ".join(sys.argv[1:])
    try:
        app()
        logger.info("命令执行完成")
    except typer.Exit:
        raise
    except KeyboardInterrupt as exc:
        logger.warning("用户中断操作")
        raise typer.Exit(code=130) from exc
    except UnauthorizedError as exc:
        feishu_notify_by_key(f"店铺 {exc.store_id} 授权过期", "dev_webhook")
        raise typer.Exit(code=1) from exc
    except Exception as exc:
        logger.exception(exc)
        extra_info = f"\n命令行: {cmd_args}"
        app_info = f"```shell\ntkauto {cmd_args}\n```"
        feishu_exception_notify(app_info, exc)
        raise typer.Exit(code=1) from exc
