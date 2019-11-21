prefix=/usr

all:

clean:
	fixme

install:
	install -d -m 0755 "$(DESTDIR)/$(prefix)/bin"
	install -m 0755 mycdn "$(DESTDIR)/$(prefix)/bin"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/sbin"
	install -m 0755 mycdn-daemon "$(DESTDIR)/$(prefix)/sbin"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib64/mycdn"
	cp -r lib/* "$(DESTDIR)/$(prefix)/lib64/mycdn"
	find "$(DESTDIR)/$(prefix)/lib64/mycdn" -type f | xargs chmod 644
	find "$(DESTDIR)/$(prefix)/lib64/mycdn" -type d | xargs chmod 755

	install -d -m 0755 "$(DESTDIR)/etc/mycdn"
	cp -r etc/* "$(DESTDIR)/etc/mycdn"
	find "$(DESTDIR)/etc/mycdn" -type f | xargs chmod 600

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib/systemd/system"
	install -m 0644 data/mycdn-daemon.service "$(DESTDIR)/$(prefix)/lib/systemd/system"

uninstall:
	rm -f "$(DESTDIR)/$(prefix)/bin/mycdn"
	rm -f "$(DESTDIR)/$(prefix)/sbin/mycdn-daemon"
	rm -f "$(DESTDIR)/$(prefix)/lib/systemd/system/mycdn-daemon.service"
	rm -rf "$(DESTDIR)/$(prefix)/lib64/mycdn"
	rm -rf "$(DESTDIR)/etc/mycdn"

.PHONY: all clean install uninstall
