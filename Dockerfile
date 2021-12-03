FROM golang

COPY watcher /watcher

CMD ["/watcher"]