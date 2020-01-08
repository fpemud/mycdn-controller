
class McPublicMirrorDatabase:

    @staticmethod
    def createFromPlugin(pluginDir, rootElem):
        ret = McPublicMirrorDatabase()
        ret.id = rootElem.prop("id")

        ret.dictOfficial = dict()
        ret.dictExtended = dict()
        if True:
            tlist1 = rootElem.xpathEval(".//filename")
            tlist2 = rootElem.xpathEval(".//classname")
            tlist3 = rootElem.xpathEval(".//json-file")
            if tlist1 != [] and tlist2 != []:
                filename = os.path.join(pluginDir, tlist1[0].getContent())
                classname = tlist2[0].getContent()
                dbObj = McUtil.loadObject(filename, classname)
                ret.dictOfficial, ret.dictExtended = dbObj.get_data()
            elif tlist3 != []:
                for e in tlist3:
                    if e.prop("type") == "official":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            jobj = json.load(f)
                            ret.dictOfficial.update(jobj)
                            ret.dictExtended.update(jobj)
                    elif e.prop("type") == "extended":
                        with open(os.path.join(pluginDir, e.getContent())) as f:
                            ret.dictExtended.update(json.load(f))
                    else:
                        raise Exception("invalid json-file")
            else:
                raise Exception("invalid metadata")

        return ret

    @staticmethod
    def createFromJson(id, jsonOfficial, jsonExtended):
        ret = McPublicMirrorDatabase()
        ret.id = id
        ret.dictOfficial = json.loads(jsonOfficial)
        ret.dictExtended = json.loads(jsonExtended)
        return ret

    def get(self, extended=False):
        if not extended:
            return self.dictOfficial
        else:
            return self.dictExtended

    def query(self, country=None, location=None, protocolList=None, extended=False):
        assert location is None or (country is not None and location is not None)
        assert protocolList is None or all(x in ["http", "ftp", "rsync"] for x in protocolList)

        # select database
        srcDict = self.dictOfficial if not extended else self.dictExtended

        # country out of scope, we don't consider this condition
        if country is not None:
            if not any(x.get("country", None) == country for x in srcDict.values()):
                country = None
                location = None

        # location out of scope, same as above
        if location is not None:
            if not any(x["country"] == country and x.get("location", None) == location for x in srcDict.values()):
                location = None

        # do query
        ret = []
        for url, prop in srcDict.items():
            if country is not None and prop.get("country", None) != country:
                continue
            if location is not None and prop.get("location", None) != location:
                continue
            if protocolList is not None and prop.get("protocol", None) not in protocolList:
                continue
            ret.append(url)
        return ret
