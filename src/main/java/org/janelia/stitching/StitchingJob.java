package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;

import mpicbg.stitching.StitchingParameters;

/**
 * @author pisarevi
 *
 */

public class StitchingJob implements Serializable {

	public enum Mode {
		Default,
		Metadata,
		NoFuse,
		FuseOnly,
		Hdf5,
		Blur
	}

	private static final long serialVersionUID = 2619120742300093982L;

	private final Mode mode;
	private transient StitchingParameters params;
	private final String baseFolder;

	private final boolean findOptimalThreshold;
	private final double crossCorrelationThreshold;
	private final int subregionSize;

	private String saveFolder;
	private String datasetName;

	private TileInfo[] tiles;
	private int dimensionality = 3;


	public StitchingJob( final StitchingArguments args )
	{
		final int modes = ( args.getMeta() ? 1 : 0 ) + ( args.getNoFuse() ? 1 : 0 ) + ( args.getFuseOnly() ? 1 : 0 );
		if ( modes > 1 )
			throw new IllegalArgumentException( "Incompatible arguments" );

		if ( args.getMeta() )
			mode = Mode.Metadata;
		else if ( args.getNoFuse() )
			mode = Mode.NoFuse;
		else if ( args.getFuseOnly() )
			mode = Mode.FuseOnly;
		else if ( args.getHdf5() )
			mode = Mode.Hdf5;
		else if ( args.getBlur() )
			mode = Mode.Blur;
		else
			mode = Mode.Default;

		if ( mode != Mode.Metadata && mode != Mode.FuseOnly ) {
			crossCorrelationThreshold = args.getCrossCorrelationThreshold();
			findOptimalThreshold = ( crossCorrelationThreshold < 0 );
		} else {
			crossCorrelationThreshold = 0;
			findOptimalThreshold = false;
		}

		if ( mode != Mode.Metadata && mode != Mode.NoFuse ) {
			subregionSize = args.getSubregionSize();
			if ( subregionSize <= 0 )
				throw new IllegalArgumentException( "Subregion size can't be negative" );
			System.out.println( "Fusion subregion size: " + subregionSize );
		} else {
			subregionSize = 0;
		}

		final File inputFile = new File( args.getInput() ).getAbsoluteFile();
		baseFolder = saveFolder = inputFile.getParent();
		datasetName = inputFile.getName();
		if ( datasetName.endsWith( ".json" ) )
			datasetName = datasetName.substring( 0, datasetName.lastIndexOf( ".json" ) );
	}

	protected StitchingJob( )
	{
		mode = Mode.Default;
		findOptimalThreshold = false;
		crossCorrelationThreshold = 0;
		subregionSize = 0;
		baseFolder = "";
	}

	public Mode getMode() {
		return mode;
	}

	public StitchingParameters getParams() {
		return params;
	}

	public void setParams( final StitchingParameters params ) {
		this.params = params;
	}

	public Map< Integer, TileInfo > getTilesMap()
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}

	public TileInfo[] getTiles() {
		return tiles;
	}

	public void setTiles( final TileInfo[] tiles ) throws Exception {
		this.tiles = tiles;
		checkTilesConfiguration();
	}

	public String getBaseFolder() {
		return baseFolder;
	}

	public String getSaveFolder() {
		return saveFolder;
	}

	public void setSaveFolder( final String saveFolder ) {
		this.saveFolder = saveFolder;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public int getDimensionality() {
		return dimensionality;
	}

	public boolean getFindOptimalThreshold() {
		return findOptimalThreshold;
	}

	public double getCrossCorrelationThreshold() {
		return crossCorrelationThreshold;
	}

	public int getSubregionSize() {
		return subregionSize;
	}

	public void validateTiles() throws IllegalArgumentException {
		if ( tiles.length < 2 )
			throw new IllegalArgumentException( "There must be at least 2 tiles in the dataset" );

		for ( int i = 0; i < tiles.length; i++ )
			if ( tiles[ i ].getPosition().length != tiles[ i ].getSize().length )
				throw new IllegalArgumentException( "Incorrect dimensionality" );

		for ( int i = 1; i < tiles.length; i++ )
			if ( tiles[ i ].numDimensions() != tiles[ i - 1 ].numDimensions() )
				throw new IllegalArgumentException( "Incorrect dimensionality" );

		// Everything is correct
		this.dimensionality = tiles[ 0 ].numDimensions();
	}

	private void checkTilesConfiguration() throws Exception
	{
		boolean malformed = ( tiles == null );
		if ( !malformed )
			for ( final TileInfo tile : tiles )
				if ( tile == null )
					malformed = true;

		if ( malformed )
			throw new NullPointerException( "Malformed input" );

		for ( int i = 0; i < tiles.length; i++ ) {
			if ( tiles[ i ].getFile() == null || tiles[ i ].getPosition() == null )
				throw new NullPointerException( "Some of required parameters are missing (file or position)" );

			if ( tiles[ i ].getIndex() == null )
				tiles[ i ].setIndex( i );
		}
	}


	// TODO: pull request for making StitchingParameters serializable, then remove it
	private void writeObject( final ObjectOutputStream stream ) throws IOException {
		stream.defaultWriteObject();
		stream.write( new Gson().toJson( params ).getBytes() );
	}
	private void readObject( final ObjectInputStream stream ) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		params = new Gson().fromJson( IOUtils.toString(stream), StitchingParameters.class );
	}
}
