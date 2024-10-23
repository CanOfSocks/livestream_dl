# livestream_dl
Garbage youtube livestream downloader
This program is in early development

# To-Do
While some components have been marked as added, testing of full functionalility may be required
- [x] Retrieval of stream url from yt-dlp
- [x] Merging of fragments into ts file
- [x] Add audio downloader with video
- [x] Download updated segments as they become available
- [x] Poll for new segments
- [x] Merge video and audio to MP4
- [ ] Test all formats where possible
- [ ] Use threading option that supports killing properly **(suggestions welcomed)**
- [ ] Output format options
- [ ] Additional resource downloads (thumbnail, info.json, description etc)
- [ ] Fix requests.exceptions.ChunkedEncodingError that appears occasionally
- [ ] Explicit handling for 403 statuses for segment download
- [ ] Print estimated and current file size for downloads
