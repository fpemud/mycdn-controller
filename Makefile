prefix=/usr

all:

clean:
	fixme

install:
	install -d -m 0755 "$(DESTDIR)/$(prefix)/sbin"
	install -m 0755 mirrors "$(DESTDIR)/$(prefix)/sbin"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib64/mirrors"
	cp -r lib/* "$(DESTDIR)/$(prefix)/lib64/mirrors"
	find "$(DESTDIR)/$(prefix)/lib64/mirrors" -type f -maxdepth 1 | xargs chmod 644
	find "$(DESTDIR)/$(prefix)/lib64/mirrors" -type d -maxdepth 1 | xargs chmod 755

	install -d -m 0755 "$(DESTDIR)/$(prefix)/share/mirrors"
	cp -r share/* "$(DESTDIR)/$(prefix)/share/mirrors"
	find "$(DESTDIR)/$(prefix)/share/mirrors" -type f | xargs chmod 644
	find "$(DESTDIR)/$(prefix)/share/mirrors" -type d | xargs chmod 755

	install -d -m 0755 "$(DESTDIR)/etc/mirrors"

	install -d -m 0755 "$(DESTDIR)/lib/systemd/system"
	install -m 0644 data/mirrors.service "$(DESTDIR)/lib/systemd/system"

uninstall:
	rm -f "$(DESTDIR)/lib/systemd/system/mirrors.service"
	rm -rf "$(DESTDIR)/etc/mirrors"
	rm -rf "$(DESTDIR)/$(prefix)/lib64/mirrors"
	rm -f "$(DESTDIR)/$(prefix)/sbin/mirrors"

.PHONY: all clean install uninstall
