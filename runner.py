import argparse
import getUrls
import download_Live


def get_Video(id, resolution='best'):
    info_dict = getUrls.get_Video_Info(id)
    download_Live.download_segments(info_dict, resolution)
    
get_Video('jGsi2eAU7MM', 'best')