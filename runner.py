import argparse
import getUrls
import download_Live


def get_Video(id, resolution='best'):
    info_dict, live_status = getUrls.get_Video_Info(id)
    download_Live.download_segments(info_dict, resolution, max_workers=2)
    
get_Video('HgesrIxxHVY', 'best')