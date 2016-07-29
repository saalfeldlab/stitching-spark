package org.janelia.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

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
		Hdf5
	}

	private static final long serialVersionUID = 2619120742300093982L;

	private Mode mode;
	private StitchingArguments args;
	private transient StitchingParameters params;
	private String baseFolder;
	private String datasetName;
	private TileInfo[] tiles;
	private int dimensionality;
	private double crossCorrelationThreshold;
	private int subregionSize;

	public StitchingJob( final StitchingArguments args ) {
		this.args = args;

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
		else
			mode = Mode.Default;

		if ( mode != Mode.Metadata && mode != Mode.FuseOnly ) {
			crossCorrelationThreshold = args.getCrossCorrelationThreshold();
			System.out.println( "Cross correlation threshold value: " + crossCorrelationThreshold );
		}

		if ( mode != Mode.Metadata && mode != Mode.NoFuse ) {
			subregionSize = args.getSubregionSize();
			if ( subregionSize <= 0 )
				throw new IllegalArgumentException( "Subregion size can't be negative" );
			System.out.println( "Fusion subregion size: " + subregionSize );
		}

		final File inputFile = new File( args.getInput() ).getAbsoluteFile();
		baseFolder = inputFile.getParent();
		datasetName = inputFile.getName();
		if ( datasetName.endsWith( ".json" ) )
			datasetName = datasetName.substring( 0, datasetName.lastIndexOf( ".json" ) );
	}

	protected StitchingJob( ) {	}

	public Mode getMode() {
		return mode;
	}

	public StitchingParameters getParams() {
		return params;
	}

	public void setParams( final StitchingParameters params ) {
		this.params = params;
	}

	public TileInfo[] getTiles() {
		return tiles;
	}

	public void setTiles( final TileInfo[] tiles ) {
		this.tiles = tiles;
	}

	public String getBaseFolder() {
		return baseFolder;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public int getDimensionality() {
		return dimensionality;
	}

	public double getCrossCorrelationThreshold() {
		return crossCorrelationThreshold;
	}

	public int getSubregionSize() {
		return subregionSize;
	}

	public void prepareTiles() throws Exception {
		loadTiles();
		setUpTiles();
	}

	private void loadTiles() throws FileNotFoundException {
		final JsonReader reader = new JsonReader( new FileReader( args.getInput() ) );
		tiles = new Gson().fromJson( reader, TileInfo[].class );

		boolean malformed = ( tiles == null );
		if ( !malformed )
			for ( final TileInfo tile : tiles )
				if ( tile == null )
					malformed = true;

		if ( malformed )
			throw new NullPointerException( "Malformed input JSON file" );
	}

	public void saveTiles( final String output ) throws IOException {
		System.out.println( "Saving updated tiles configuration to " + output );
		final FileWriter writer = new FileWriter( output );
		writer.write( new Gson().toJson( tiles ) );
		writer.close();
	}

	public void savePairwiseShifts( final List< SerializablePairWiseStitchingResult > shifts, final String output ) throws IOException {
		System.out.println( "Saving pairwise shifts to " + output );
		final FileWriter writer = new FileWriter( output );
		writer.write( new Gson().toJson( shifts) );
		writer.close();
	}

	public List< SerializablePairWiseStitchingResult > loadPairwiseShifts( final String input ) throws FileNotFoundException {
		final JsonReader reader = new JsonReader( new FileReader( input ) );
		final SerializablePairWiseStitchingResult[] shifts = new Gson().fromJson( reader, SerializablePairWiseStitchingResult[].class );
		return new ArrayList< SerializablePairWiseStitchingResult >( Arrays.asList( shifts ) );
	}

	private void setUpTiles() throws Exception {

		for ( int i = 0; i < tiles.length; i++ ) {
			if ( tiles[ i ].getFile() == null || tiles[ i ].getPosition() == null )
				throw new NullPointerException( "Some of required parameters are missing (file or position)" );

			if ( tiles[ i ].getIndex() == null )
				tiles[ i ].setIndex( i );
		}
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
