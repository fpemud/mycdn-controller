prefix=/usr

all:

clean:
	fixme

install:
	install -d -m 0755 "$(DESTDIR)/$(prefix)/sbin"
	install -m 0755 mycdn-controller "$(DESTDIR)/$(prefix)/sbin"

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib/mycdn-controller"
	cp -r lib/* "$(DESTDIR)/$(prefix)/lib/mycdn-controller"
	find "$(DESTDIR)/$(prefix)/lib/mycdn-controller" -type f | xargs chmod 644
	find "$(DESTDIR)/$(prefix)/lib/mycdn-controller" -type d | xargs chmod 755

	install -d -m 0755 "$(DESTDIR)/etc/mycdn-controller"
	cp -r etc/* "$(DESTDIR)/etc/mycdn-controller"
	find "$(DESTDIR)/etc/mycdn-controller" -type f | xargs chmod 600

	install -d -m 0755 "$(DESTDIR)/$(prefix)/lib/systemd/system"
	install -m 0644 data/mycdn-controller.service "$(DESTDIR)/$(prefix)/lib/systemd/system"

uninstall:
	rm -f "$(DESTDIR)/$(prefix)/sbin/mycdn-controller"
	rm -f "$(DESTDIR)/$(prefix)/lib/systemd/system/mycdn-controller.service"
	rm -rf "$(DESTDIR)/$(prefix)/lib/mycdn-controller"
	rm -rf "$(DESTDIR)/etc/mycdn-controller"

.PHONY: all clean install uninstall
