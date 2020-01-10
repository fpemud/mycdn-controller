prefix=/usr

all:

clean:
	fixme

install:
	install -d -m 0755 "$(DESTDIR)/$(prefix)/sbin"
	install -m 0755 mirrors "$(DESTDIR)/$(prefix)/sbin"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib64/mirrors"
	cp -r lib/* "$(DESTDIR)/$(prefix)/lib64/mirrors"
	find "$(DESTDIR)/$(prefix)/lib64/mirrors" -type f | xargs chmod 644
	find "$(DESTDIR)/$(prefix)/lib64/mirrors" -type d | xargs chmod 755

	install -d -m 0755 "$(DESTDIR)/etc/mirrors"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib/systemd/system"
	install -m 0644 data/mirrors.service "$(DESTDIR)/$(prefix)/lib/systemd/system"

uninstall:
	rm -f "$(DESTDIR)/$(prefix)/sbin/mirrors"
	rm -f "$(DESTDIR)/$(prefix)/lib/systemd/system/mirrors.service"
	rm -rf "$(DESTDIR)/$(prefix)/lib64/mirrors"
	rm -rf "$(DESTDIR)/etc/mirrors"

.PHONY: all clean install uninstall
