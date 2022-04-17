import gzip
import struct

class TAQTradesReader(object):
    
    '''
    This reader reads an entire compressed binary TAQ trades file into memory,
    uncompresses it, and gives its clients access to the contents of the file
    via a set of get methods.
    '''


    def __init__(self, filePathName ):
        '''
        Do all of the heavy lifting here and give users getters for the results.
        '''
        self.filePathName = filePathName
        with gzip.open( filePathName, 'rb') as f:
            file_content = f.read()
            self.file_content = file_content
            self._header = struct.unpack_from(">2i",file_content[0:8])
            endI = 8 + ( 4 * self._header[1] )
            self._ts = struct.unpack_from( ( ">%di" % self._header[ 1 ] ), file_content[ 8:endI ] )
            startI = endI
            endI = endI + ( 4 * self._header[1] )
            self._s = struct.unpack_from( ( ">%di" % self._header[ 1 ] ), file_content[ startI:endI ] )
            startI = endI
            endI = endI + ( 4 * self._header[1] )
            self._p = struct.unpack_from( ( ">%df" % self._header[ 1 ] ), file_content[ startI:endI ] )

    def getN(self):
        return self._header[1]
    
    def getSecsFromEpocToMidn(self):
        return self._header[0]
    
    def getPrice( self, index ):
        return self._p[ index ]
    
    def getMillisFromMidn( self, index ):
        return self._ts[ index ]
    
    def getTimestamp(self, index ):
        return self.getMillisFromMidn( index ) # Compatibility 
    
    def getSize( self, index ):
        return self._s[ index ]
    
    def rewrite( self, filePathName, tickerId ):
        s = struct.Struct( ">QHIf" ) 
        out = gzip.open( filePathName, "wb" )
        baseTS = self.getSecsFromEpocToMidn() * 1000
        for i in range( self.getN() ):
            ts = baseTS + self.getMillisFromMidn( i )
            out.write( s.pack( ts, tickerId, self.getSize(i), self.getPrice(i) ) )
        out.close()

    def rewriteCleaned( self,ts, prices, sizes, filePathName):
        out = gzip.open( filePathName, "wb" )
        out.write(struct.pack(">2i",self.getSecsFromEpocToMidn(), len(ts)))
        out.write(struct.pack(">%di" % len(ts), *ts))
        out.write(struct.pack(">%di" % len(sizes), *sizes))
        out.write(struct.pack(">%df" % len(prices), *prices))
        out.close()

    def rewrite_adj( self, filePathName, adj_s, adj_p):
        out = gzip.open( filePathName, "wb" )
        header = [self.getSecsFromEpocToMidn(), self.getN()]
        out.write(struct.pack(">2i", *header))
        ts_list = []
        s_list = []
        p_list = []
        for i in range( self.getN() ):
            ts_list.append(self.getMillisFromMidn( i ))
            s_list.append(int(self.getSize(i) * adj_s))
            p_list.append(self.getPrice(i) / adj_p)
        out.write(struct.pack( ">%di" % header[1], *ts_list) )
        out.write(struct.pack(">%di" % header[1], *s_list))
        out.write(struct.pack(">%df" % header[1], *p_list))
        out.close()