/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.util;

import org.junit.Test;

import java.nio.charset.Charset;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by Vlad Medvedev on 21.01.2016.
 * vladislav.medvedev@devfactory.com
 */
public class CharsetDetectorTest {
    public static final String DEFAULT_ENCODING = "UTF-8";

    @Test
    public void testGuessEncodingByMozilla() throws Exception {
        assertThat(CharsetDetector.guessEncodingByMozilla(encode("KOI8-R", "привет")), is("KOI8-R"));
        assertThat(CharsetDetector.guessEncodingByMozilla(encode("Windows-1251", "привет")), is("WINDOWS-1251"));
        assertThat(CharsetDetector.guessEncodingByMozilla(encode("ISO-8859-7", "Πάντ' ἀγαθὰ πράττω, ὦ φίλε.")), is("ISO-8859-7"));
        assertThat(CharsetDetector.guessEncodingByMozilla(encode("Windows-1252", "hello")), is(DEFAULT_ENCODING));
    }


    @Test
    public void testGuessEncoding() throws Exception {
        assertThat(CharsetDetector.guessEncoding(encode("KOI8-R", "привет")), is("KOI8-R"));
        assertThat(CharsetDetector.guessEncoding(encode("Windows-1251", "привет")), is("WINDOWS-1251"));
        assertThat(CharsetDetector.guessEncoding(encode("ISO-8859-7", "Πάντ' ἀγαθὰ πράττω, ὦ φίλε.")), is("ISO-8859-7"));
        assertThat(CharsetDetector.guessEncoding(encode("Windows-1252", "hello")), is(DEFAULT_ENCODING));
    }

    private byte[] encode(String charset, String text) {
        return Charset.forName(charset).encode(text).array();
    }
}