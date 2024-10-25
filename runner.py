import argparse
import getUrls
import download_Live


def get_Video(id, resolution='best'):
    info_dict, live_status = getUrls.get_Video_Info(id)
    #if live_status == 'is_live' or live_status == 'was_live':
    download_Live.download_segments(info_dict, resolution, max_workers=5)
    
get_Video('zZ14NQfYJH0', 'best')