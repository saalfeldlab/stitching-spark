package org.janelia.dataaccess;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfo;

import com.google.gson.GsonBuilder;

import ij.ImagePlus;

/**
 * Provides access to data on the used storage system.
 *
 * @author Igor Pisarev
 */
public interface DataProvider
{
	public DataProviderType getType();

	public boolean fileExists( final String link ) throws IOException;
	public void createFolder( final String link ) throws IOException;

	public void copyFile( final String srcLink, final String dstLink ) throws IOException;
	public void copyFolder( final String srcLink, final String dstLink ) throws IOException;

	public void moveFile( final String srcLink, final String dstLink ) throws IOException;
	public void moveFolder( final String srcLink, final String dstLink ) throws IOException;

	public void deleteFile( final String link ) throws IOException;
	public void deleteFolder( final String link ) throws IOException;

	public InputStream getInputStream( final String link ) throws IOException;
	public OutputStream getOutputStream( final String link ) throws IOException;

	public ImagePlus loadImage( final String link ) throws IOException;
	public void saveImage( final ImagePlus imp, final String link ) throws IOException;

	public Reader getJsonReader( final String link ) throws IOException;
	public Writer getJsonWriter( final String link ) throws IOException;

	public TileInfo[] loadTiles( final String link ) throws IOException;
	public void saveTiles( final TileInfo[] tiles, final String link ) throws IOException;

	public List< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String link ) throws IOException;
	public void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > pairwiseShifts, final String link ) throws IOException;

	public N5Reader createN5Reader( final String baseLink ) throws IOException;
	public N5Writer createN5Writer( final String baseLink ) throws IOException;

	public N5Reader createN5Reader( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException;
	public N5Writer createN5Writer( final String baseLink, final GsonBuilder gsonBuilder ) throws IOException;
}
