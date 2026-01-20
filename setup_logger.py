
import logging
import logging.handlers

from typing import Union, Optional

import os

import os
import logging
from typing import Union

# Verbose setup
VERBOSE_LEVEL_NUM = 15
VERBOSE_LEVEL_NAME = "VERBOSE"
ENV_DISABLE_FLAG = "DISABLE_LIVESTREAM_DL_VERBOSE_LOGGING"

def setup_logging(
    log_level="INFO",
    console=True,
    file: str | None = None,
    force=False,
    file_options: dict = None,
    logger: logging.Logger | None = None,
    logger_name: str | None = None,
    video_id: str | None = None,
    metadata: dict | None = None  # New parameter for dynamic stages
) -> Union[logging.Logger, logging.LoggerAdapter]:
    """
    Configure logging with dynamic stages based on metadata.
    """
    file_options = file_options or {}
    metadata = metadata or {}

    # Standard Windows console fix
    def disable_quick_edit():
        import ctypes
        kernel32 = ctypes.windll.kernel32
        hStdin = kernel32.GetStdHandle(-10)
        mode = ctypes.c_ulong()
        kernel32.GetConsoleMode(hStdin, ctypes.byref(mode))
        mode.value &= ~0x40
        kernel32.SetConsoleMode(hStdin, mode)

    if os.name == "nt":
        disable_quick_edit()

    if logger is None:
        logger = logging.getLogger(logger_name)

    if force:
        for h in list(logger.handlers):
            logger.removeHandler(h)
    
    if not logger.handlers:
        level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(level)

        # 1. Build the dynamic format string
        # Start with the level
        format_parts = ["%(levelname)s"]

        # Add video_id if it exists
        if video_id:
            format_parts.append("%(video_id)s")

        # 2. Map dictionary keys to [key] stages
        for key in metadata.keys():
            format_parts.append(f"%({key})s")

        # Join parts with brackets and add timestamp/message
        # Result example: "[INFO] [VID123] [Stage1] [UserA] 2024-..."
        fmt_prefix = " ".join([f"[{p}]" for p in format_parts])
        fmt_str = f"{fmt_prefix} %(asctime)s - %(message)s"
            
        formatter = logging.Formatter(fmt_str, datefmt='%Y-%m-%d %H:%M:%S')

        if logger_name:
            logger.propagate = False

        if console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        if file:
            # (File handler logic remains the same...)
            if file_options.get("maxBytes"):
                handler = logging.handlers.RotatingFileHandler(
                    file, maxBytes=file_options.get("maxBytes"), 
                    backupCount=file_options.get("backupCount", 5), encoding='utf-8'
                )
            elif file_options.get("when"):
                handler = logging.handlers.TimedRotatingFileHandler(
                    file, when=file_options.get("when"), 
                    interval=file_options.get("interval", 1), 
                    backupCount=file_options.get("backupCount", 7), encoding='utf-8'
                )
            else:
                handler = logging.FileHandler(file, mode='a', encoding='utf-8')
            
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    # 3. Wrap in Adapter if video_id OR metadata exists
    if video_id or metadata:
        extra_info = {}
        if video_id:
            extra_info["video_id"] = video_id
        
        # Merge the metadata dictionary into the extra info
        extra_info.update(metadata)
        
        return logging.LoggerAdapter(logger, extra_info)
    
    return logger


def _install_verbose():
    """
    Registers the VERBOSE level. 
    Ensures idempotency (only runs once) and respects environment variables.
    """
    
    # Check environment variable
    if os.getenv(ENV_DISABLE_FLAG, "false").lower() == "true":
        return

    # 2. Safety Check: Only add if it doesn't already exist
    if hasattr(logging, VERBOSE_LEVEL_NAME):
        return

    # Add the name to the logging system
    logging.addLevelName(VERBOSE_LEVEL_NUM, VERBOSE_LEVEL_NAME)
    
    # Add a constant to the logging module (e.g., logging.VERBOSE)
    setattr(logging, VERBOSE_LEVEL_NAME, VERBOSE_LEVEL_NUM)

    # 3. Define the Logger method: logger.verbose(...)
    def verbose_method(self, message, *args, **kws):
        if self.isEnabledFor(VERBOSE_LEVEL_NUM):
            self._log(VERBOSE_LEVEL_NUM, message, args, **kws)

    # 4. Define the Global function: logging.verbose(...)
    def verbose_global(message, *args, **kws):
        """Log a message with severity 'VERBOSE' on the root logger."""
        logging.log(VERBOSE_LEVEL_NUM, message, *args, **kws)

    # 5. Apply the patches
    logging.Logger.verbose = verbose_method
    logging.verbose = verbose_global

# Execute on import
_install_verbose()