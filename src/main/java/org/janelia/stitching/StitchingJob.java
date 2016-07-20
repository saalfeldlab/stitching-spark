package org.janelia.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import mpicbg.stitching.StitchingParameters;

/**
 * @author pisarevi
 *
 */

public class StitchingJob implements Serializable {
	
	private static final int DefaultFusionSubregionSize = 512;
	
	public enum Mode {
		Default,
		NoFuse,
		FuseOnly
	}
	
	private static final long serialVersionUID = 2619120742300093982L;

	private Mode mode;
	private StitchingArguments args;
	private transient StitchingParameters params;
	private String baseFolder; 
	private TileInfo[] tiles;
	private int dimensionality;
	private int subregionSize;
	
	public StitchingJob( final StitchingArguments args ) {
		this.args = args;
		
		if ( args.getNoFuse() && args.getFuseOnly() )
			throw new IllegalArgumentException( "Incompatible arguments" );
		
		if ( args.getNoFuse() )
			mode = Mode.NoFuse;
		else if ( args.getFuseOnly() )
			mode = Mode.FuseOnly;
		else
			mode = Mode.Default;
		
		if ( mode != Mode.NoFuse ) {
			subregionSize = args.getSubregionSize();
			if ( subregionSize <= 0 ) {
				subregionSize = DefaultFusionSubregionSize;
				System.out.println( "No subregion size for fusion is present, using default one (" + subregionSize + ")" );
			}
		}
		
		baseFolder = new File( args.getInput() ).getAbsoluteFile().getParent();
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
	
	public int getDimensionality() {
		return dimensionality;
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
		final FileWriter writer = new FileWriter( output );
		writer.write( new Gson().toJson( tiles ) );
		writer.close();
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
			if ( tiles[ i ].getDimensionality() != tiles[ i - 1 ].getDimensionality() )
				throw new IllegalArgumentException( "Incorrect dimensionality" );
		
		// Everything is correct
		this.dimensionality = tiles[ 0 ].getDimensionality();
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
